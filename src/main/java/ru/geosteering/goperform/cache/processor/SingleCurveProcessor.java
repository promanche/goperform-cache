package ru.geosteering.goperform.cache.processor;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.commonModels.dataService.responses.DataEndMessage;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.model.ws.*;
import ru.geosteering.goperform.cache.nats.ConnectionEventListener;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
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
    private final AtomicReference<LoadStatus> loadStatus = new AtomicReference<>(LoadStatus.UNKNOWN);
    private final TreeSet<CurveItem> realItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> historyItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> loadBuffer = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final Map<Integer, List<CurveSegment>> segmentCache = new HashMap<>(); // scale -> segments
    private final Map<Integer, CurveItem> lastSegmentItem = new HashMap<>(); // scale -> lastItem

    @Getter
    private volatile CurveItem lastSaved;
    @Getter
    private volatile CurveItem firstSaved;
    @Getter
    private final AtomicInteger savedCount = new AtomicInteger();

    private boolean haveRestRequest;
    private boolean isActive;

    private final ReloadData reloadData = new ReloadData();

    private long pointTimer;
    @Setter
    private long requestTimer;

    public SingleCurveProcessor(ExtraCurveInfo info, CurveDispatcher dispatcher) {
        this.info = info;
        this.dispatcher = dispatcher;

        isApproximated = info.getIndexType() != LogIndexType.MEASURED_DEPTH && info.getAxisDefinition() == null
                && (info.getTypeLogData() == LogDataType.DOUBLE || info.getTypeLogData() == LogDataType.LONG);

        reloadSavedInfo();
        loadLost();

        if (loadStatus.get() == LoadStatus.UNKNOWN) {
            doRequest(false);
        }
    }

    @Override
    public void onConnect() {
        synchronized (loadStatus) {
            if (loadStatus.get() == LoadStatus.UNKNOWN) {
                doRequest(false);
            }
        }
    }

    @Override
    public void onDisconnect() {

        loadStatus.getAndUpdate(loadStatus -> {
            switch (loadStatus) {
                case BLOCKED:
                    return LoadStatus.BLOCKED;
                case LOADED:
                    return LoadStatus.LOADED;
                default:
                    return LoadStatus.UNKNOWN;
            }
        });

        realItemCache.clear();
        historyItemCache.clear();
        loadBuffer.clear();
    }

    public void onCurveDataMessage(CurveDataMessage message, boolean isReal) {

        CurveItem item = CurveItem.fromAbstractDataItem(message.getData(), info.getIndexType() != LogIndexType.MEASURED_DEPTH);

        if (isReal) {
            isActive = true;

            if (lastSaved == null || Double.compare(item.getKey(), lastSaved.getKey()) > 0) {
                collect(item, true);
                updateInfo(item);

                synchronized (loadStatus) {
                    if (loadStatus.get() == LoadStatus.UNKNOWN) {
                        doRequest(false);
                    }
                }

                sendWsMessage(new PointMessage(info.getId(), item.getKey(), item.getValue()));
            } else {
                updateReloadData(item.getKey(), 5);
            }
        } else {
            collect(item, false);
        }
    }

    public void onDataEndMessage(DataEndMessage message) {

        int sent = message.getSentCount();
        int received = loadBuffer.size();
        int step = 5;

        while (sent != received && step > 0) {
            log.warn("Curve {} received {}. Waiting for last points.....", info.getId(), received);
            LockSupport.parkUntil(30 + System.currentTimeMillis());
            step--;
            received = loadBuffer.size();
        }

        dispatcher.incrementHistCount(received);

        if (sent == 0) {
            if (isActive) {
                realItemCache.addAll(historyItemCache);
                historyItemCache.clear();
            }

            loadStatus.getAndUpdate(loadStatus -> loadStatus == LoadStatus.BLOCKED ? LoadStatus.BLOCKED : LoadStatus.LOADED);
            sendWsMessage(new LoadedMessage(info.getId()));
            log.info("Curve {} data loaded, {}", info.getId(), message);

        } else if (sent != received) {
            log.error("Curve {} received count {} not equals to sent {}", info.getId(), received, sent);
            doRequest(false);

        } else {
            long millis = Math.max(1, System.currentTimeMillis() - pointTimer);
            long pointsPerSecond = received * 1000L / millis;
            log.info("Curve {} received {} items with avg speed {} points/sec. Request->firstPoint {} ms, firstPoint->lastPoint {} ms",
                    info.getId(), received, pointsPerSecond, pointTimer - requestTimer, millis);

            historyItemCache.addAll(loadBuffer);
            saveHistoryItems();
            updateInfoFromBuffer();
            createSegments(loadBuffer);
            sendWsMessage(new PartMessage(info.getId(), loadBuffer.first().getKey(), loadBuffer.last().getKey()));
            doRequest(false);
        }
    }

    public Set<Integer> getScaleSet() {
        return Set.copyOf(segmentCache.keySet());
    }

    public List<?> getCurveData(Double from, Double to, Integer scale) {

        haveRestRequest = true;

        if (scale != null && segmentCache.containsKey(scale)) {

            List<CurveSegment> result;
            synchronized (segmentCache.get(scale)) {
                result = dispatcher.getRepository().getSegmentsFromTo(info.getId(), scale, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveSegment>>) str -> StaticMapper.parseListOf(str, CurveSegment.class).stream())
                        .collect(Collectors.toList());

                result.addAll(segmentCache.get(scale));

            }
            return result;

        } else {
            List<CurveItem> result = dispatcher.getRepository().getItemsFromTo(info.getId(), from, to)
                    .stream()
                    .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                    .collect(Collectors.toList());

            double finalFrom = from == null ? Double.MIN_VALUE : from;
            double finalTo = to == null ? Double.MAX_VALUE : to;

            synchronized (historyItemCache) {
                historyItemCache.stream()
                        .filter(item -> Double.compare(item.getKey(), finalFrom) >= 0 && Double.compare(item.getKey(), finalTo) <= 0)
                        .forEachOrdered(result::add);
            }

            synchronized (realItemCache) {
                realItemCache.stream()
                        .filter(item -> Double.compare(item.getKey(), finalFrom) >= 0 && Double.compare(item.getKey(), finalTo) <= 0)
                        .forEachOrdered(result::add);
            }

            return result;
        }
    }

    public void updateReloadData(Double from, int delayMinutes) {

        synchronized (reloadData) {
            loadStatus.set(LoadStatus.BLOCKED);
            reloadData.from = reloadData.from != null && Double.compare(reloadData.from, from) < 0 ? reloadData.from : from;
            reloadData.reloadTime = LocalDateTime.now().plusMinutes(delayMinutes);
        }
    }

    public void reload() {
        synchronized (reloadData) {
            if (reloadData.reloadTime != null && reloadData.reloadTime.isBefore(LocalDateTime.now()) && loadBuffer.isEmpty()) {

                log.info("Curve {} will be reload from {}", info.getId(), reloadData.from);

                clearData(reloadData.from);
                loadLost();

                reloadData.reloadTime = null;
                reloadData.from = null;

                doRequest(true);
            }
        }
    }

    private void clearData(Double from) {

        loadBuffer.clear();
        historyItemCache.clear();
        realItemCache.clear();
        segmentCache.clear();
        lastSegmentItem.clear();

        if (lastSaved != null && Double.compare(lastSaved.getKey(), reloadData.from) >= 0) {
            dispatcher.getRepository().deleteItems(info.getId(), from);
            reloadSavedInfo();
            if (isApproximated) {
                Double segFrom = lastSaved == null ? null : lastSaved.getKey() + 0.0000001;
                dispatcher.getRepository().deleteSegments(info.getId(), segFrom);
            }
        }
    }

    private void reloadSavedInfo() {
        firstSaved = dispatcher.getRepository().getFirstItem(info.getId()).orElse(null);
        lastSaved = dispatcher.getRepository().getLastItem(info.getId()).orElse(null);
        savedCount.set(dispatcher.getRepository().getItemsRecords(info.getId()) * dispatcher.getConfig().BATCH_SIZE);
    }

    private void collect(CurveItem item, boolean isReal) {
        if (isReal) {
            synchronized (realItemCache) {
                realItemCache.add(item);
                saveRealItems();
            }
        } else {
            synchronized (loadBuffer) {
                if (loadBuffer.isEmpty()) {
                    pointTimer = System.currentTimeMillis();
                    loadStatus.compareAndSet(LoadStatus.IN_QUEUE, LoadStatus.IN_PROGRESS);
                }
                loadBuffer.add(item);
            }
        }
    }

    private void saveRealItems() {

        if (loadStatus.get() == LoadStatus.LOADED) {

            CurveItem tmpFirst = firstSaved;
            CurveItem tmpLast;
            int tmpCount = 0;

            while (realItemCache.size() >= dispatcher.getConfig().BATCH_SIZE + dispatcher.getConfig().MARGIN_SIZE) {

                ArrayList<CurveItem> itemsBatch = new ArrayList<>(dispatcher.getConfig().BATCH_SIZE);
                for (int i = 0; i < dispatcher.getConfig().BATCH_SIZE; i++) {
                    itemsBatch.add(realItemCache.pollFirst());
                }

                dispatcher.getRepository().saveItems(List.of(ItemDto.fromItemsList(info.getId(), itemsBatch)));

                if (tmpFirst == null) {
                    tmpFirst = itemsBatch.get(0);
                }
                tmpLast = itemsBatch.get(itemsBatch.size() - 1);
                tmpCount += itemsBatch.size();

                dispatcher.getRepository().saveOrUpdateInfo(info);

                firstSaved = tmpFirst;
                lastSaved = tmpLast;
                savedCount.addAndGet(tmpCount);

                createSegments(itemsBatch);
            }
        }
    }

    private void saveHistoryItems() {

        synchronized (historyItemCache) {
            List<ItemDto> transfer = new ArrayList<>();

            CurveItem tmpFirst = firstSaved;
            CurveItem tmpLast = lastSaved;
            int tmpCount = 0;


            while (historyItemCache.size() >= dispatcher.getConfig().BATCH_SIZE) {

                ArrayList<CurveItem> itemsBatch = new ArrayList<>(dispatcher.getConfig().BATCH_SIZE);

                for (int i = 0; i < dispatcher.getConfig().BATCH_SIZE; i++) {
                    itemsBatch.add(historyItemCache.pollFirst());
                }

                transfer.add(ItemDto.fromItemsList(info.getId(), itemsBatch));

                if (tmpFirst == null) {
                    tmpFirst = itemsBatch.get(0);
                }
                tmpLast = itemsBatch.get(itemsBatch.size() - 1);
                tmpCount += itemsBatch.size();
            }

            dispatcher.getRepository().saveItems(transfer);

            firstSaved = tmpFirst;
            lastSaved = tmpLast;
            savedCount.addAndGet(tmpCount);
        }
    }

    private void updateInfo(CurveItem item) {

        synchronized (info) {

            Double key = item.getKey();

            if (info.getMinKey() == null || Double.compare(info.getMinKey(), key) > 0) {
                info.setMinKey(key);
            }

            if (info.getMaxKey() == null || Double.compare(info.getMaxKey(), key) < 0) {
                info.setMaxKey(key);
                info.setLastValue(item.getValue());
            }

            if (info.getAxisDefinition() == null && (info.getTypeLogData() == LogDataType.DOUBLE || info.getTypeLogData() == LogDataType.LONG)) {
                Double value = (Double) item.getValue();

                if (info.getMinValue() == null || Double.compare(info.getMinValue(), value) > 0) {
                    info.setMinValue(value);
                }

                if (info.getMaxValue() == null || Double.compare(info.getMaxValue(), value) < 0) {
                    info.setMaxValue(value);
                }
            }
        }
    }

    private void updateInfoFromBuffer() {

        synchronized (info) {

            if (info.getMinKey() == null || Double.compare(info.getMinKey(), loadBuffer.first().getKey()) > 0) {
                info.setMinKey(loadBuffer.first().getKey());
            }

            if (info.getMaxKey() == null || Double.compare(info.getMaxKey(), loadBuffer.last().getKey()) < 0) {
                info.setMaxKey(loadBuffer.last().getKey());
                info.setLastValue(loadBuffer.first().getValue());
            }

            if (info.getAxisDefinition() == null && (info.getTypeLogData() == LogDataType.DOUBLE || info.getTypeLogData() == LogDataType.LONG)) {

                for (CurveItem item : loadBuffer) {
                    double value = (Double) item.getValue();
                    if (info.getMinValue() == null || Double.compare(info.getMinValue(), value) > 0) {
                        info.setMinValue(value);
                    }
                    if (info.getMaxValue() == null || Double.compare(info.getMaxValue(), value) < 0) {
                        info.setMaxValue(value);
                    }
                }
            }
            dispatcher.getRepository().saveOrUpdateInfo(info);
        }

    }

    private void doRequest(boolean ifBlocked) {
        loadBuffer.clear();

        if (loadStatus.get() != LoadStatus.BLOCKED || ifBlocked) {
            String from = findFrom();
            String to = findTo();

            CurveDispatcher.RequestType type = haveRestRequest ? CurveDispatcher.RequestType.LOAD_REST : CurveDispatcher.RequestType.LOAD_ACTIVE;

            CurveDataRequest request =
                    new CurveDataRequest(
                            info.getId(),
                            from,
                            to,
                            null,
                            false,
                            false,
                            dispatcher.getConfig().HISTORY_REQUEST_LIMIT,
                            dispatcher.getConfig().HISTORY_NUID + "." + info.getId()
                    );

            dispatcher.addRequestTask(new CurveDispatcher.RequestTask(request, type));
            loadStatus.set(LoadStatus.IN_QUEUE);
        }
    }

    private String findFrom() {

        Double key = historyItemCache.isEmpty() ? lastSaved == null ? null : lastSaved.getKey() : historyItemCache.last().getKey();
        return getKeyAsString(key, info.getIndexType());
    }

    private String findTo() {

        Double key = realItemCache.isEmpty() ? null : realItemCache.first().getKey();
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
        dispatcher.getWebSocketMessageProcessor().sendMessage(info.getId(), message);
    }

    private void createSegments(Collection<CurveItem> items) {

        if (isApproximated && lastSaved != null && firstSaved != null) {
            for (Integer scale : dispatcher.getConfig().SCALE_MINUTES) {

                int itemsOnPixel = findItemsOnPixel(scale);

                if (isApproximatedScale(itemsOnPixel)) {

                    createScaleSegments(items, scale, itemsOnPixel);

                } else {
                    log.debug("Curve id {} items {} NOT ADDED for approximating in scale {} with density {} points/pxl", info.getId(), items.size(), scale, itemsOnPixel);
                }
            }
        }
    }

    private boolean isApproximatedScale(int itemsOnPixel) {
        return itemsOnPixel >= 5;
    }

    private int findItemsOnPixel(int scale) {

        int totalSeconds = (int) ((lastSaved.getKey() - firstSaved.getKey()) / 1000);
        int secondsOnPixel = scale * 60 / 120;

        return totalSeconds == 0 ? 0 : (int) ((long) secondsOnPixel * savedCount.get() / totalSeconds);
    }

    private void createScaleSegments(Collection<CurveItem> items, int scale, int itemsOnPixel) {

        List<CurveSegment> segments = segmentCache.computeIfAbsent(scale, k -> new ArrayList<>());

        synchronized (segments) {
            CurveSegment lastSegment = segments.isEmpty() ? null : segments.get(segments.size() - 1);

            int secondsOnPixel = scale * 60 / 120;

            for (CurveItem item : items) {

                if (lastSegment != null && item.getKey() - lastSegment.getFirstKey() < (secondsOnPixel - 1) * 1000) {

                    lastSegment.addItem(item);

                } else {

                    CurveSegment segment = new CurveSegment();

                    CurveItem lastItem = lastSegmentItem.get(scale);
                    if (lastItem != null && item.getKey() - lastItem.getKey() < (secondsOnPixel - 1) * 1000) {
                        segment.addItem(lastItem);
                    }

                    segment.addItem(item);

                    segments.add(segment);

                    lastSegment = segment;
                }

                lastSegmentItem.put(scale, item);
            }

            if (segments.size() > 1) {

                segments.remove(segments.size() - 1);

                saveSegments(segments, scale);

                log.debug("{} segments saved: curve id {}, scale {}, seconds/pxl {}, points/pxl {}", segments.size(), info.getId(), scale, secondsOnPixel, itemsOnPixel);

                segments.clear();

                segments.add(lastSegment);
            }
        }
    }

    private void saveSegments(List<CurveSegment> segments, int scale) {

        List<SegmentDto> transfer = new ArrayList<>();

        int first = 0;

        while (segments.size() - first > dispatcher.getConfig().BATCH_SIZE) {
            transfer.add(SegmentDto.fromLinesList(info.getId(), scale, segments.subList(first, first + dispatcher.getConfig().BATCH_SIZE)));
            first = first + dispatcher.getConfig().BATCH_SIZE;
        }

        transfer.add(SegmentDto.fromLinesList(info.getId(), scale, segments.subList(first, segments.size())));

        dispatcher.getRepository().saveSegments(transfer);
    }

    private void loadLost() {

        if (isApproximated && lastSaved != null && firstSaved != null) {

            Map<Integer, Double> scaleLast = dispatcher.getRepository().getScalesLast(info.getId());

            for (Integer scale : dispatcher.getConfig().SCALE_MINUTES) {
                if (isApproximatedScale(findItemsOnPixel(scale))) {
                    scaleLast.putIfAbsent(scale, Double.MIN_VALUE);
                }
            }

            Double from = scaleLast.values().stream().min(Double::compareTo).orElse(Double.MIN_VALUE);

            List<CurveItem> items = dispatcher.getRepository().getItemsFromTo(info.getId(), from, Double.MAX_VALUE)
                    .stream()
                    .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                    .collect(Collectors.toList());

            log.info("{} lost items for {} loaded", items.size(), info.getId());

            scaleLast.forEach((scale, last) -> {
                List<CurveItem> lost = items.stream()
                        .filter(i -> Double.compare(i.getKey(), last) >= 0)
                        .collect(Collectors.toList());

                if (!lost.isEmpty()) {
                    lastSegmentItem.put(scale, lost.get(0));
                    createScaleSegments(lost, scale, findItemsOnPixel(scale));
                }
            });
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
