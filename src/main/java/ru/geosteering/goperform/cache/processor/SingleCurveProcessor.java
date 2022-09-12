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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SingleCurveProcessor implements ConnectionEventListener {

    @Getter
    private final ExtraCurveInfo info;
    private final boolean isApproximated;

    @Getter
    private final AtomicReference<LoadStatus> loadStatus = new AtomicReference<>(LoadStatus.UNKNOWN);

    private final TreeSet<CurveItem> realItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> historyItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> loadBuffer = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final Map<Integer, List<CurveSegment>> segmentCache = new HashMap<>(); // scale -> segments
    private final Map<Integer, CurveItem> lastSegmentItem = new HashMap<>(); // scale -> lastItem

    private final CurveDispatcher dispatcher;

    @Setter
    private boolean haveRestRequest;
    @Getter
    private CurveItem lastSaved;
    @Getter
    private CurveItem firstSaved;
    @Getter
    private int savedCount;
    @Getter
    private Double minKey;
    @Getter
    private Double maxKey;
    @Getter
    private Object lastValue;
    private boolean isActive;
    private final ReloadData reloadData = new ReloadData();

    private long pointTimer;
    @Setter
    private long requestTimer;

    public SingleCurveProcessor(ExtraCurveInfo info, CurveDispatcher dispatcher) {
        this.info = info;
        this.dispatcher = dispatcher;

        minKey = info.getIndexType() == LogIndexType.MEASURED_DEPTH ? info.getMdMin() : info.getTimeMin().toInstant().toEpochMilli();
        maxKey = info.getIndexType() == LogIndexType.MEASURED_DEPTH ? info.getMdMax() : info.getTimeMax().toInstant().toEpochMilli();

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

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        int sent = message.getSentCount();
        int received = loadBuffer.size();

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
            log.error("Received count {} not equals to sent {}", received, sent);
            doRequest(false);

        } else {
            long millis = System.currentTimeMillis() - pointTimer;
            long pointsPerSecond = received * 1000L / millis;
            log.info("Curve {} received {} items with avg speed {} points/sec. Request->firstPoint {} ms, firstPoint->lastPoint {} ms",
                    info.getId(), received, pointsPerSecond, pointTimer - requestTimer, millis);

            historyItemCache.addAll(loadBuffer);
            saveHistoryItems();
            updateInfo();
            doRequest(false);
            sendWsMessage(new PartMessage(info.getId(), loadBuffer.first().getKey(), loadBuffer.last().getKey()));
        }

        loadBuffer.clear();
    }

    public Set<Integer> getScaleSet() {
        return Set.copyOf(segmentCache.keySet());
    }

    public List<CurveItem> getCurveData(Double from, Double to) {

        List<CurveItem> result = dispatcher.getRepository().getItemsFromTo(info.getId(), from, to)
                .stream()
                .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                .collect(Collectors.toList());

        double finalFrom = from == null ? Double.MIN_VALUE : from;
        double finalTo = to == null ? Double.MAX_VALUE : to;

        historyItemCache.stream()
                .filter(item -> Double.compare(item.getKey(), finalFrom) >= 0 && Double.compare(item.getKey(), finalTo) <= 0)
                .forEachOrdered(result::add);

        realItemCache.stream()
                .filter(item -> Double.compare(item.getKey(), finalFrom) >= 0 && Double.compare(item.getKey(), finalTo) <= 0)
                .forEachOrdered(result::add);

        return result;
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

        if (Double.compare(lastSaved.getKey(), reloadData.from) >= 0) {
            dispatcher.getRepository().deleteItems(info.getId(), from);
            reloadSavedInfo();
            Double segFrom = lastSaved == null ? null : lastSaved.getKey() + 0.0000001;
            dispatcher.getRepository().deleteSegments(info.getId(), segFrom);
        }
    }

    private void reloadSavedInfo() {
        firstSaved = dispatcher.getRepository().getFirstItem(info.getId()).orElse(null);
        lastSaved = dispatcher.getRepository().getLastItem(info.getId()).orElse(null);
        savedCount = dispatcher.getRepository().getItemsRecords(info.getId()) * dispatcher.getConfig().BATCH_SIZE;

        dispatcher.getRepository().getInfo(info.getId()).ifPresent(in -> {
            info.setMaxValue(in.getMaxValue());
            info.setMinValue(in.getMinValue());
        });
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

            while (realItemCache.size() >= dispatcher.getConfig().BATCH_SIZE + dispatcher.getConfig().MARGIN_SIZE) {

                ArrayList<CurveItem> itemsBatch = new ArrayList<>(dispatcher.getConfig().BATCH_SIZE);
                for (int i = 0; i < dispatcher.getConfig().BATCH_SIZE; i++) {
                    itemsBatch.add(realItemCache.pollFirst());
                }

                dispatcher.getRepository().saveItems(List.of(ItemDto.fromItemsList(info.getId(), itemsBatch)));

                if (firstSaved == null) {
                    firstSaved = itemsBatch.get(0);
                }

                lastSaved = itemsBatch.get(itemsBatch.size() - 1);

                savedCount += itemsBatch.size();

                dispatcher.getRepository().saveOrUpdateInfo(info);

                createSegments(itemsBatch);
            }
        }
    }

    private void saveHistoryItems() {

        List<ItemDto> transfer = new ArrayList<>();

        while (historyItemCache.size() >= dispatcher.getConfig().BATCH_SIZE) {

            ArrayList<CurveItem> itemsBatch = new ArrayList<>(dispatcher.getConfig().BATCH_SIZE);

            for (int i = 0; i < dispatcher.getConfig().BATCH_SIZE; i++) {
                itemsBatch.add(historyItemCache.pollFirst());
            }

            transfer.add(ItemDto.fromItemsList(info.getId(), itemsBatch));

            if (firstSaved == null) {
                firstSaved = itemsBatch.get(0);
            }

            lastSaved = itemsBatch.get(itemsBatch.size() - 1);

            savedCount += itemsBatch.size();

            createSegments(itemsBatch);
        }

        dispatcher.getRepository().saveItems(transfer);
    }

    private void updateInfo(CurveItem item) {

        synchronized (info) {

            Double key = item.getKey();

            if (minKey == null || Double.compare(minKey, key) > 0) {
                minKey = key;
            }

            if (maxKey == null || Double.compare(maxKey, key) < 0) {
                maxKey = key;
                lastValue = item.getValue();
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

    private void updateInfo() {

        synchronized (info) {

            if (minKey == null || Double.compare(minKey, loadBuffer.first().getKey()) > 0) {
                minKey = loadBuffer.first().getKey();
            }

            if (maxKey == null || Double.compare(maxKey, loadBuffer.last().getKey()) < 0) {
                maxKey = loadBuffer.last().getKey();
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
        }

        dispatcher.getRepository().saveOrUpdateInfo(info);
    }

    private void doRequest(boolean ifBlocked) {

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

        CurveItem lastHistory = historyItemCache.isEmpty() ? null : historyItemCache.last();

        Double key = lastHistory == null ? lastSaved == null ? null : lastSaved.getKey() : lastHistory.getKey();

        if (key == null) {
            return null;
        }

        return getKeyAsString(key, info.getIndexType());
    }

    private String findTo() {

        CurveItem firstReal = realItemCache.isEmpty() ? null : realItemCache.first();
        Double key = firstReal == null ? null : firstReal.getKey();

        if (key == null) {
            return null;
        }

        return getKeyAsString(firstReal.getKey(), info.getIndexType());
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

    private void createSegments(List<CurveItem> items) {

        if (isApproximated) {
            for (Map.Entry<Integer, List<CurveSegment>> entry : segmentCache.entrySet()) {

                Integer scale = entry.getKey();
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

        return (int) ((long) secondsOnPixel * savedCount / totalSeconds);
    }

    private void createScaleSegments(List<CurveItem> items, int scale, int itemsOnPixel) {

        List<CurveSegment> segments = segmentCache.computeIfAbsent(scale, k -> new ArrayList<>());

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
