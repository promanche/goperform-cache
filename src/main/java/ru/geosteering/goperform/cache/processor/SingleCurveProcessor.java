package ru.geosteering.goperform.cache.processor;

import com.google.common.util.concurrent.AtomicDouble;
import io.nats.client.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.exception.NullResponseException;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.model.ws.*;
import ru.geosteering.goperform.cache.nats.ConnectionEventListener;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;
import ru.geosteering.goperform.cache.repository.dto.SegmentDto;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SingleCurveProcessor implements ConnectionEventListener {

    @Getter
    private final ExtraCurveInfo info;
    private final CurveDispatcher dispatcher;

    private final boolean isApproximated;
    @Getter
    private final boolean isDateTimeCurve;

    @Getter
    private LoadStatus loadStatus = LoadStatus.UNKNOWN;
    private final TreeSet<CurveItem> realItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> historyItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> loadBuffer = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final Map<Integer, List<CurveSegment>> segmentCache = new HashMap<>(); // scale -> segments
    private final AtomicInteger historyPoints = new AtomicInteger();

    @Getter
    private CurveItem lastSaved;
    @Getter
    private CurveItem firstSaved;
    @Getter
    private int savedCount;

    @Setter
    private boolean fromRest;
    private boolean isActive;

    private final ReloadData reloadData = new ReloadData();

    private long pointTimer;
    private long requestTimer;

    /**
     * Миллисекунды последней записи в логе об ошибках вычисления минимумов-максимумов (предположительно, на некорретных данных)
     */
    private long lastMinMaxErrorReported;
    /**
     * Счётчик ошибок, сообщения о которых были заблокированы во время {@link #MINMAX_ERROR_REPORT_THRESHOLD}
     */
    private int minMaxErrorCounter;
    /**
     * Промежуток времени после {@link #lastMinMaxErrorReported}, на который блокируются последующие сообщения
     */
    private static final long MINMAX_ERROR_REPORT_THRESHOLD = 3 * 60 * 1000L;

    private long lastBlockedLogTime = 0;
    private long lastUpdateReloadLogTime = 0;

    public SingleCurveProcessor(ExtraCurveInfo info, boolean fromRest, CurveDispatcher dispatcher) {
        this.info = info;
        this.fromRest = fromRest;
        this.dispatcher = dispatcher;

        isDateTimeCurve = info.getIndexType() != LogIndexType.MEASURED_DEPTH;

        isApproximated = isDateTimeCurve && info.getAxisDefinition() == null
                && (info.getTypeLogData() == LogDataType.DOUBLE || info.getTypeLogData() == LogDataType.LONG);

        reloadSavedInfo();
        loadLost();
        addRequestJob(false);
    }

    @Override
    public synchronized void onConnect() {
        if (loadStatus == LoadStatus.UNKNOWN) {
            addRequestJob(false);
        }
    }

    @Override
    public synchronized void onDisconnect() {
        if (loadStatus != LoadStatus.BLOCKED) {
            loadStatus = LoadStatus.UNKNOWN;
        }
        realItemCache.clear();
        historyItemCache.clear();
        loadBuffer.clear();
    }

    public synchronized void onCurveDataMessage(CurveDataMessage message, boolean isReal) {
        CurveItem item = CurveItem.fromAbstractDataItem(message.getData(), isDateTimeCurve);

        if (isReal) {

            if (keyNotInRange(item.getKey())) {
                log.warn("Curve {} received point outside the allowed range: {}", info.getId(), StaticMapper.toJson(item));
                return;
            }

            isActive = true;
            if (lastSaved == null || Double.compare(item.getKey(), lastSaved.getKey()) > 0) {
                collect(item, true);
                updateInfo(item);
                sendWsMessage(new PointMessage(info.getId(), item.getKey(), item.getValue()));

            } else {
                updateReloadData(item, 5);
            }

        } else {
            collect(item, false);
        }
    }

    private boolean keyNotInRange(Double key) {
        double minVal = isDateTimeCurve ? dispatcher.config.MIN_TIME_MILLIS : dispatcher.config.MIN_DEPTH_METERS;
        double maxVal = isDateTimeCurve ? OffsetDateTime.now().plusHours(24).toInstant().toEpochMilli() : dispatcher.config.MAX_DEPTH_METERS;
        return Double.compare(key, minVal) < 0 || Double.compare(key, maxVal) > 0;
    }

    public synchronized void onDataEndMessage(DataEndMessage message) {
        int sent = message.getSentCount();
        int received = historyPoints.getAndSet(0);
        dispatcher.incrementHistCount(received);

        if (sent == 0) {
            if (isActive) {
                realItemCache.addAll(historyItemCache);

                //------------ Костыль до перехода на джобы (оставляем и после перехода) ------------
                AtomicDouble from = new AtomicDouble(-1);
                AtomicDouble to = new AtomicDouble(-1);
                AtomicInteger count = new AtomicInteger(0);
                realItemCache.removeIf(item -> {
                    boolean alreadySaved = lastSaved != null && Double.compare(lastSaved.getKey(), item.getKey()) >= 0;
                    if (alreadySaved) {
                        if (from.get() == -1 || Double.compare(item.getKey(), from.get()) < 0) {
                            from.set(item.getKey());
                        }
                        if (to.get() == -1 || Double.compare(item.getKey(), to.get()) > 0) {
                            to.set(item.getKey());
                        }
                        count.incrementAndGet();
                    }
                    return alreadySaved;
                });
                if (count.get() > 0) {
                    log.warn("Curve {} duplicate points found: from {}, to {}, count {}", info.getId(), from.get(), to.get(), count.get());
                }
                //-------------------------------------------------------

                historyItemCache.clear();
            }
            loadStatus = loadStatus == LoadStatus.BLOCKED ? LoadStatus.BLOCKED : LoadStatus.LOADED;
            sendWsMessage(new LoadedMessage(info.getId()));
            log.info("Curve {} data loaded, {}", info.getId(), message);

        } else if (sent != received) {
            log.error("Curve {} received count {} not equals to sent {}", info.getId(), received, sent);
            addRequestJob(false);

        } else {
            if (received != loadBuffer.size()) {
                log.error("Buffer size {} not equals to received {}", loadBuffer.size(), received);
            }

            long millis = Math.max(1, System.currentTimeMillis() - pointTimer);
            long pointsPerSecond = received * 1000L / millis;
            log.info("Curve {} received {} items with avg speed {} points/sec. Request->firstPoint {} ms, firstPoint->lastPoint {} ms",
                    info.getId(), received, pointsPerSecond, pointTimer - requestTimer, millis);

            historyItemCache.addAll(loadBuffer);
            saveHistoryItems();
            updateInfoFromBuffer();
            new SegmentCreator()
                    .createSegments(loadBuffer)
                    .logResults("onDataEndMessage()");
            sendWsMessage(new PartMessage(info.getId(), loadBuffer.first().getKey(), loadBuffer.last().getKey()));
            addRequestJob(false);
        }
    }

    public synchronized Set<Integer> getScaleSet() {
        return Set.copyOf(segmentCache.keySet());
    }

    public synchronized List<?> getCurveData(Double from, Double to, Integer scale) {
        if (scale != null && scale > 15) {
            List<CurveSegment> result = dispatcher.repository.getSegmentsFromTo(info.getId(), scale, from, to)
                    .stream()
                    .flatMap((Function<String, Stream<CurveSegment>>) str -> StaticMapper.parseListOf(str, CurveSegment.class).stream())
                    .collect(Collectors.toList());
            if (segmentCache.containsKey(scale)) {
                result.addAll(segmentCache.get(scale));
            }
            new SegmentCreator()
                    .addSegmentsFromItems(getTail(from, to), scale, result)
                    .logResults("getCurveData()");

            return result;

        } else {
            List<CurveItem> result = dispatcher.repository.getItemsFromTo(info.getId(), from, to)
                    .stream()
                    .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                    .collect(Collectors.toList());

            result.addAll(getTail(from, to));

            return result;
        }
    }

    public synchronized List<CurveItem> getTail(Double from, Double to) {
        List<CurveItem> result = new ArrayList<>();

        double finalFrom = from == null ? Double.MIN_VALUE : from;
        double finalTo = to == null ? Double.MAX_VALUE : to;

        historyItemCache.stream()
                .filter(item -> Double.compare(item.getKey(), finalFrom) >= 0 && Double.compare(item.getKey(), finalTo) <= 0)
                .forEach(result::add);


        realItemCache.stream()
                .filter(item -> Double.compare(item.getKey(), finalFrom) >= 0 && Double.compare(item.getKey(), finalTo) <= 0)
                .forEach(result::add);

        return result;
    }

    public synchronized List<CurveSegment> getSegmentsFromTail(Double from, Double to, int scale) {
        List<CurveSegment> result = new ArrayList<>();
        new SegmentCreator()
                .addSegmentsFromItems(getTail(from, to), scale, result)
                .logResults("getSegmentsFromTail()");
        return result;
    }

    public synchronized void updateReloadData(CurveItem item, int delayMinutes) {
        Double from = item.getKey();

        if (loadStatus != LoadStatus.BLOCKED) {
            log.warn("Curve {} is blocked for next reloading in {} minutes from {}", info.getId(), delayMinutes, from);
        } else if (System.currentTimeMillis() - lastUpdateReloadLogTime > 60000) {
            log.info("Curve {} blocking extended by {} minutes due to point {}", info.getId(), delayMinutes, item);
            lastUpdateReloadLogTime = System.currentTimeMillis();
        }

        loadStatus = LoadStatus.BLOCKED;
        reloadData.from = reloadData.from != null && Double.compare(reloadData.from, from) < 0 ? reloadData.from : from;
        reloadData.reloadTime = LocalDateTime.now().plusMinutes(delayMinutes);
        dispatcher.removeFromRequestQueue(info.getId());
    }

    public synchronized void updateReloadData() {
        loadStatus = LoadStatus.BLOCKED;
        reloadData.from = 0.0;
        reloadData.reloadTime = LocalDateTime.now();
        dispatcher.removeFromRequestQueue(info.getId());
    }

    public synchronized void reload() {
        if (loadStatus == LoadStatus.BLOCKED) {
            boolean reloadTimeNotNull = reloadData.reloadTime != null;
            boolean reloadTimeIsCome = reloadTimeNotNull && reloadData.reloadTime.isBefore(LocalDateTime.now());
            boolean loadBufferIsEmpty = loadBuffer.isEmpty();
            if (reloadTimeNotNull && reloadTimeIsCome && loadBufferIsEmpty) {
                log.info("Curve {} will now be reloaded from {}", info.getId(), reloadData.from);
                clearData(reloadData.from);
                loadLost();
                reloadData.reloadTime = null;
                reloadData.from = null;
                addRequestJob(true);
            } else if (System.currentTimeMillis() - lastBlockedLogTime > 60000) {
                String reason;
                if (!reloadTimeNotNull) {
                    reason = "reload time is null";
                } else if (!reloadTimeIsCome) {
                    reason = "reload time has not come yet (expected at " + reloadData.reloadTime + ")";
                } else {
                    reason = "load buffer is not empty (size = " + loadBuffer.size() + ")";
                }
                log.info("Curve {} is still blocked by reason of {}", info.getId(), reason);

                lastBlockedLogTime = System.currentTimeMillis();
            }
        }
    }

    private void clearData(Double from) {
        loadBuffer.clear();
        historyItemCache.clear();
        realItemCache.clear();
        segmentCache.clear();

        if (lastSaved != null && Double.compare(lastSaved.getKey(), reloadData.from) >= 0) {
            dispatcher.repository.deleteItems(info.getId(), from);
            reloadSavedInfo();
            if (isApproximated) {
                Double segFrom = lastSaved == null ? null : lastSaved.getKey() + 0.0000001;
                dispatcher.repository.deleteSegments(info.getId(), segFrom);
            }
        }
    }

    private void reloadSavedInfo() {
        firstSaved = dispatcher.repository.getFirstItem(info.getId()).orElse(null);
        lastSaved = dispatcher.repository.getLastItem(info.getId()).orElse(null);
        if (firstSaved != null) {
            info.setMinLoadedKey(firstSaved.getKey());
        }
        if (lastSaved != null) {
            info.setMaxLoadedKey(lastSaved.getKey());
        }
        savedCount = dispatcher.repository.getItemsRecordsCount(info.getId()) * dispatcher.config.BATCH_SIZE;
    }

    private void collect(CurveItem item, boolean isReal) {
        if (isReal) {
            realItemCache.add(item);
            saveRealItems();

        } else {
            if (historyPoints.getAndIncrement() == 0) {
                pointTimer = System.currentTimeMillis();
                loadStatus = loadStatus == LoadStatus.IN_QUEUE ? LoadStatus.IN_PROGRESS : loadStatus;
            }
            loadBuffer.add(item);
        }
    }

    private void saveRealItems() {
        if (loadStatus == LoadStatus.LOADED) {

            CurveItem tmpFirst = firstSaved;
            CurveItem tmpLast;
            int tmpCount = 0;

            SegmentCreator segmentCreator = new SegmentCreator();
            while (realItemCache.size() >= dispatcher.config.BATCH_SIZE + dispatcher.config.MARGIN_SIZE) {
                ArrayList<CurveItem> itemsBatch = new ArrayList<>(dispatcher.config.BATCH_SIZE);
                for (int i = 0; i < dispatcher.config.BATCH_SIZE; i++) {
                    itemsBatch.add(realItemCache.pollFirst());
                }
                dispatcher.repository.saveItems(List.of(ItemDto.fromItemsList(info.getId(), itemsBatch)));

                if (tmpFirst == null) {
                    tmpFirst = itemsBatch.get(0);
                }
                tmpLast = itemsBatch.get(itemsBatch.size() - 1);
                tmpCount += itemsBatch.size();

                dispatcher.repository.saveOrUpdateInfo(info);

                firstSaved = tmpFirst;
                lastSaved = tmpLast;
                savedCount = savedCount + tmpCount;

                segmentCreator.createSegments(itemsBatch);
            }
            segmentCreator.logResults("saveRealItems()");
        }
    }

    private void saveHistoryItems() {
        List<ItemDto> transfer = new ArrayList<>();

        CurveItem tmpFirst = firstSaved;
        CurveItem tmpLast = lastSaved;
        int tmpCount = 0;

        while (historyItemCache.size() >= dispatcher.config.BATCH_SIZE) {
            ArrayList<CurveItem> itemsBatch = new ArrayList<>(dispatcher.config.BATCH_SIZE);

            for (int i = 0; i < dispatcher.config.BATCH_SIZE; i++) {
                itemsBatch.add(historyItemCache.pollFirst());
            }

            transfer.add(ItemDto.fromItemsList(info.getId(), itemsBatch));

            if (tmpFirst == null) {
                tmpFirst = itemsBatch.get(0);
            }
            tmpLast = itemsBatch.get(itemsBatch.size() - 1);
            tmpCount += itemsBatch.size();
        }

        dispatcher.repository.saveItems(transfer);
        firstSaved = tmpFirst;
        lastSaved = tmpLast;
        savedCount = savedCount + tmpCount;

    }

    private void updateInfo(CurveItem item) {
        Double key = item.getKey();
        if (info.getMinKey() == null || Double.compare(info.getMinKey(), key) > 0) {
            info.setMinKey(key);
        }

        if (info.getMaxKey() == null || Double.compare(info.getMaxKey(), key) <= 0) {
            info.setMaxKey(key);
            info.setLastValue(String.valueOf(item.getValue()));
        }

        if (info.getMaxLoadedKey() == null || Double.compare(info.getMaxLoadedKey(), key) <= 0) {
            info.setMaxLoadedKey(key);
        }

        if (info.getMinLoadedKey() == null || Double.compare(info.getMinLoadedKey(), key) > 0) {
            info.setMinLoadedKey(key);
        }

        updateMaxMinValue(item);
    }

    private void updateInfoFromBuffer() {
        if (info.getMinKey() == null || Double.compare(info.getMinKey(), loadBuffer.first().getKey()) > 0) {
            info.setMinKey(loadBuffer.first().getKey());
        }

        if (info.getMaxKey() == null || Double.compare(info.getMaxKey(), loadBuffer.last().getKey()) <= 0) {
            info.setMaxKey(loadBuffer.last().getKey());
            info.setLastValue(String.valueOf(loadBuffer.first().getValue()));
        }

        if (info.getMaxLoadedKey() == null || Double.compare(info.getMaxLoadedKey(), loadBuffer.last().getKey()) <= 0) {
            info.setMaxLoadedKey(loadBuffer.last().getKey());
        }

        if (info.getMinLoadedKey() == null || Double.compare(info.getMinLoadedKey(), loadBuffer.first().getKey()) > 0) {
            info.setMinLoadedKey(loadBuffer.first().getKey());
        }

        updateMaxMinValue(loadBuffer.toArray(new CurveItem[0]));
        dispatcher.repository.saveOrUpdateInfo(info);
    }

    private void updateMaxMinValue(CurveItem... items) {
        if (info.getAxisDefinition() == null && (info.getTypeLogData() == LogDataType.DOUBLE || info.getTypeLogData() == LogDataType.LONG)) {
            for (CurveItem item : items) {
                try {
                    double value = ((Number) item.getValue()).doubleValue();

                    if (info.getMinValue() == null || Double.compare(info.getMinValue(), value) > 0) {
                        info.setMinValue(value);
                    }
                    if (info.getMaxValue() == null || Double.compare(info.getMaxValue(), value) < 0) {
                        info.setMaxValue(value);
                    }
                } catch (Exception e) {
                    if (System.currentTimeMillis() - lastMinMaxErrorReported > MINMAX_ERROR_REPORT_THRESHOLD) {
                        log.info("updateMaxMinValue for {} threw {}: {}, data: {}. {} more messages suppressed."
                                , getInfo().getId()
                                , e.getClass().getName()
                                , e.getMessage()
                                , StaticMapper.toJson(item)
                                , minMaxErrorCounter
                        );
                        minMaxErrorCounter = 0;
                        lastMinMaxErrorReported = System.currentTimeMillis();
                    } else {
                        minMaxErrorCounter++;
                    }

                    log.debug(e.getMessage(), e);
                }
            }
        }
    }

    private void addRequestJob(boolean ifBlocked) {
        loadBuffer.clear();
        if (loadStatus != LoadStatus.BLOCKED || ifBlocked) {
            CurveDispatcher.RequestType requestType = fromRest ? CurveDispatcher.RequestType.LOAD_REST : CurveDispatcher.RequestType.LOAD_ACTIVE;
            dispatcher.addRequestTask(new CurveDispatcher.RequestTask(info.getId(), requestType, this::doItemsRequest));
            loadStatus = LoadStatus.IN_QUEUE;
        }
    }

    private synchronized void doItemsRequest() {
        String from = findFrom();
        String to = findTo();
        CurveDataRequest request =
                new CurveDataRequest(
                        info.getId(),
                        from,
                        to,
                        null,
                        false,
                        false,
                        dispatcher.config.HISTORY_REQUEST_LIMIT,
                        dispatcher.config.HISTORY_NUID + "." + info.getId()
                );

        log.info("Request: {}", request);
        requestTimer = System.currentTimeMillis();
        Message response = NatsConnector.sendRequest(dispatcher.config.SUBJECT, StaticMapper.toBytes(request));
        if (response != null) {
            log.debug("Response: {}", new String(response.getData()));

            ApiMessage apiMessage = StaticMapper.parseObject(new String(response.getData()), ApiMessage.class);
            switch (Objects.requireNonNull(apiMessage).getType()) {
                case CURVE_INFO -> {
                }
                case STATUS -> {
                    StatusMessage statusMessage = (StatusMessage) apiMessage;
                    if (statusMessage.getStatus() != EResult.OK) {
                        log.error("Error curveData request for {}, message {}", info.getId(), statusMessage);
                        dispatcher.onErrorDataRequest(info.getId());
                    }
                }
                default -> {
                    log.error("Unknown response {}", new String(response.getData()));
                    dispatcher.onErrorDataRequest(info.getId());
                }
            }
        } else {
            log.error("Response is null");
            addRequestJob(false);
            throw new NullResponseException();
        }
    }

    private String findFrom() {
        Double key = historyItemCache.isEmpty() ? lastSaved == null ? null : lastSaved.getKey() : historyItemCache.last().getKey();
        if (key == null) {
            key = isDateTimeCurve ? dispatcher.config.MIN_TIME_MILLIS : dispatcher.config.MIN_DEPTH_METERS;
        }
        return getKeyAsString(key, info.getIndexType());
    }

    private String findTo() {
        Double key = realItemCache.isEmpty() ? null : realItemCache.first().getKey();
        if (key == null) {
            key = isDateTimeCurve ? OffsetDateTime.now().plusHours(1).toInstant().toEpochMilli() : dispatcher.config.MAX_DEPTH_METERS;
        }
        return getKeyAsString(key, info.getIndexType());
    }

    private String getKeyAsString(Double key, LogIndexType type) {
        if (key == null) {
            return null;
        }
        return type == LogIndexType.MEASURED_DEPTH ?
                new BigDecimal(key).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() :
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
    }

    private void sendWsMessage(WsMessage message) {
        dispatcher.webSocketMessageProcessor.sendMessage(info.getId(), message);
    }

    private boolean isApproximatedScale(Integer scale) {
        return scale != null && scale >= 15;
    }

    private int findItemsOnPixel(int scale) {
        int totalSeconds = (int) ((lastSaved.getKey() - firstSaved.getKey()) / 1000);
        int secondsOnPixel = scale * 60 / 120;

        return totalSeconds == 0 ? 0 : (int) ((long) secondsOnPixel * savedCount / totalSeconds);
    }

    /**
     * Обёртка, высчитывающая количество ошибок процедуры вычисления штрихов ("сегментации") из-за некорректных точек.
     * По окончании сегментации вызывающий код может вывести единичное сообщение о числе ошибок, вызвав {@link #logResults(String)}.
     */
    private class SegmentCreator {
        private int totalItems;
        private int invalidItems;

        public void logResults(String label) {
            if (invalidItems > 0) {
                log.info("Curve {} segmentation for {}, items invalid/total: {}/{}", info.getId(), label, invalidItems, totalItems);
            }
        }

        public SegmentCreator createSegments(Collection<CurveItem> items) {
            if (isApproximated && lastSaved != null && firstSaved != null) {
                for (Integer scale : dispatcher.config.SCALE_MINUTES) {
                    int itemsOnPixel = findItemsOnPixel(scale);
                    if (isApproximatedScale(scale)) {
                        createScaleSegments(items, scale, itemsOnPixel);

                    } else {
                        log.debug("Curve id {} items {} NOT ADDED for approximating in scale {} with density {} points/pxl", info.getId(), items.size(), scale, itemsOnPixel);
                    }
                }
            }
            return this;
        }

        private void createScaleSegments(Collection<CurveItem> items, int scale, int itemsOnPixel) {
            List<CurveSegment> segments = segmentCache.computeIfAbsent(scale, k -> new ArrayList<>());
            addSegmentsFromItems(items, scale, segments);
            if (segments.size() > 1) {
                CurveSegment last = segments.remove(segments.size() - 1);
                saveSegments(segments, scale);
                log.debug("{} segments saved: curve id {}, scale {}, seconds/pxl {}, points/pxl {}", segments.size(), info.getId(), scale, scale * 60 / 120, itemsOnPixel);
                segments.clear();
                segments.add(last);
            }
        }

        public SegmentCreator addSegmentsFromItems(Collection<CurveItem> items, int scale, List<CurveSegment> segments) {
            CurveSegment lastSegment = segments.isEmpty() ? null : segments.get(segments.size() - 1);
            for (CurveItem item : items) {
                try {
                    totalItems++;
                    if (lastSegment != null && Double.compare(item.getKey(), lastSegment.getFirstKey()) >= 0 && Double.compare(item.getKey(), lastSegment.getLastKey()) <= 0) {
                        lastSegment.addItem(item);

                    } else {
                        CurveSegment segment = new CurveSegment(item, scale);
                        if (lastSegment != null && Double.compare(lastSegment.getLastKey(), segment.getFirstKey()) == 0) {
                            lastSegment.addItem(item);
                        }
                        segment.addItem(item);
                        segments.add(segment);
                        lastSegment = segment;
                    }
                } catch (Exception e) {
                    if (invalidItems == 0) {
                        log.info("Create segment exception. Message: {}, item: {}", e.getMessage(), StaticMapper.toJson(item));
                    }
                    invalidItems++;
                    log.debug(e.getMessage(), e);
                }
            }
            return this;
        }
    }

    private void saveSegments(List<CurveSegment> segments, int scale) {
        List<SegmentDto> transfer = new ArrayList<>();
        int first = 0;
        while (segments.size() - first > dispatcher.config.BATCH_SIZE) {
            transfer.add(SegmentDto.fromLinesList(info.getId(), scale, segments.subList(first, first + dispatcher.config.BATCH_SIZE)));
            first = first + dispatcher.config.BATCH_SIZE;
        }

        transfer.add(SegmentDto.fromLinesList(info.getId(), scale, segments.subList(first, segments.size())));
        dispatcher.repository.saveSegments(transfer);
    }

    private void loadLost() {
        if (isApproximated && lastSaved != null && firstSaved != null) {
            Map<Integer, Double> scaleLast = dispatcher.repository.getScalesLast(info.getId());

            for (Integer scale : dispatcher.config.SCALE_MINUTES) {
                if (isApproximatedScale(scale)) {
                    scaleLast.putIfAbsent(scale, Double.MIN_VALUE);
                }
            }

            Double from = scaleLast.values().stream().min(Double::compareTo).orElse(Double.MIN_VALUE);

            List<CurveItem> items = dispatcher.repository.getItemsFromTo(info.getId(), from, Double.MAX_VALUE)
                    .stream()
                    .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                    .toList();

            log.info("{} lost items for {} loaded", items.size(), info.getId());

            SegmentCreator segmentCreator = new SegmentCreator();
            scaleLast.forEach((scale, last) -> {
                List<CurveItem> lost = items.stream()
                        .filter(i -> Double.compare(i.getKey(), last) >= 0)
                        .collect(Collectors.toList());

                if (!lost.isEmpty()) {
                    segmentCreator.createScaleSegments(lost, scale, findItemsOnPixel(scale));
                }
            });
            segmentCreator.logResults("loadLost()");
        }
    }

    public enum LoadStatus {
        IN_QUEUE, IN_PROGRESS, BLOCKED, LOADED, UNKNOWN
    }

    private static class ReloadData {
        private Double from;
        private LocalDateTime reloadTime;
    }
}
