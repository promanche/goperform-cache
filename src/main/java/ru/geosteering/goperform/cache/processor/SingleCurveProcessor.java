package ru.geosteering.goperform.cache.processor;

import com.google.common.util.concurrent.AtomicDouble;
import io.nats.client.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.commonModels.dataService.responses.DataEndMessage;
import ru.geosteering.commonModels.dataService.responses.StatusMessage;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.exception.NullResponseException;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.model.ws.LoadedMessage;
import ru.geosteering.goperform.cache.model.ws.PartMessage;
import ru.geosteering.goperform.cache.model.ws.PointMessage;
import ru.geosteering.goperform.cache.model.ws.WsMessage;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.processor.request.RequestTask;
import ru.geosteering.goperform.cache.processor.request.RequestType;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Обработчик одной конкретной кривой с уникальным id. Все публичные методы синхронизированы.
 * <p>Принимает из диспетчера для обработки два вида сообщений {@link CurveDataMessage}  и {@link DataEndMessage}.
 * {@link CurveDataMessage} могут поступать из 2-х источников: данные в реальном времени и исторические данные по запросу.
 * После создания обработчик автоматически отправляет запрос на загрузку исторических данных.
 *
 * <p>{@link LoadStatus} показывает статус загрузки исторических данных по кривой.
 * {@linkplain LoadStatus#UNKNOWN UNKNOWN} дефолтный статус при создании обработчика,
 * {@linkplain LoadStatus#IN_QUEUE IN_QUEUE} запрос на загрузку исторических данных добавлен в очередь загрузки,
 * {@linkplain LoadStatus#IN_PROGRESS IN_PROGRESS} запрос на загрузку исторических данных выполняется,
 * {@linkplain LoadStatus#LOADED LOADED} исторические данные загружены.
 *
 * <p>Запросы истории выполняются пакетно с лимитом точек
 * {@linkplain ru.geosteering.goperform.cache.config.Config#HISTORY_REQUEST_LIMIT HISTORY_REQUEST_LIMIT}.
 * Во время загрузки очередного пакета точки собираются в {@linkplain #loadBuffer буфер загрузки}.
 * Окончанием загрузки пакета считается {@link DataEndMessage}.
 * Если количество загруженных точек соответствует указанному в {@link DataEndMessage},
 * то они отправляются в {@link #historyItemCache} для последующей обработки.
 * В противном случае точки из буфера игнорируются.
 * Если получен {@link DataEndMessage} с sentCount = 0, считаем что исторические данные полностью загружены (LoadStatus=LOADED).
 *
 * <p>Точки в реальном времени собираются в {@link #realItemCache}. Пока история не загружена полностью {@link #realItemCache} просто копит точки.
 * После загрузки истории последние точки из {@link #historyItemCache} передаем в {@link #realItemCache} и далее по мере накопления сохраняем в БД.
 *
 * <p>Сохранение точек в БД происходит пачками по {@linkplain ru.geosteering.goperform.cache.config.Config#BATCH_SIZE BATCH_SIZE} штук в jsonb формате.
 *
 * <p>При сохранении очередного пакета точек в БД также сохраняется обновленная информация по кривой.
 *
 * <p>При сохранении очередного пакета точек в БД также создаются и сохраняются наборы отрезков для кривых по заданным шкалам сегментации.
 * См. {@link SegmentProcessor}.
 *
 * <p>Механизм автоматической перезагрузки данных.
 * Если в реалтайм получена точка старше {@link #lastSaved}, кривая подлежит перезагрузке. См. {@link #reload()}
 */
@Slf4j
public class SingleCurveProcessor {

    /**
     * Промежуток времени после {@link #lastMinMaxErrorReported}, на который блокируются последующие сообщения
     */
    private static final long MINMAX_ERROR_REPORT_THRESHOLD = 3 * 60 * 1000L;
    @Getter
    protected final ExtraCurveInfo info;
    protected final CurveDispatcher dispatcher;
    @Getter
    protected final SegmentProcessor segmentProcessor;
    private final Config config;
    private final boolean isApproximated;
    @Getter
    private final boolean isDateTimeCurve;
    private final TreeSet<CurveItem> realItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> historyItemCache = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final TreeSet<CurveItem> loadBuffer = new TreeSet<>(Comparator.comparing(CurveItem::getKey));
    private final AtomicInteger historyPoints = new AtomicInteger();
    @Getter
    private LoadStatus loadStatus = LoadStatus.UNKNOWN;
    @Getter
    private CurveItem lastSaved;
    @Getter
    private CurveItem firstSaved;
    @Getter
    private int savedCount;
    @Setter
    private boolean fromRest;
    private boolean isReload;
    @Getter
    private ReloadData reloadData;
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
    private long lastBlockedLogTime = 0;
    private long lastUpdateReloadLogTime = 0;
    private int suppressedOldPoints = 0;

    public SingleCurveProcessor(ExtraCurveInfo info, boolean fromRest, CurveDispatcher dispatcher) {
        this.info = info;
        this.fromRest = fromRest;
        this.dispatcher = dispatcher;

        this.config = dispatcher.config;
        isDateTimeCurve = info.getIndexType() != LogIndexType.MEASURED_DEPTH;

        isApproximated = isDateTimeCurve && isNumeric();

        reloadSavedInfo();
        segmentProcessor = new SegmentProcessor(info, dispatcher.config, this, dispatcher.repository);
        segmentProcessor.restoreScaledSegments();
        addRequestJob();
    }

    private void toggleLoadStatus(LoadStatus newStatus) {
        LoadStatus oldStatus = loadStatus;
        if (oldStatus == newStatus) {
            return;
        }
        log.debug("Curve {} status {} -> {}", getInfo().getId(), oldStatus, newStatus);
        loadStatus = newStatus;
    }

    public long totalBufferSize() {
        return realItemCache.size() + historyItemCache.size() + loadBuffer.size();
    }

    public synchronized void onConnect() {
        addRequestJob();
    }

    public synchronized void onDisconnect() {
        toggleLoadStatus(LoadStatus.UNKNOWN);
        realItemCache.clear();
        historyItemCache.clear();
        loadBuffer.clear();
    }

    public synchronized void onCurveDataMessage(CurveDataMessage message, boolean isReal) {
        CurveItem item = mapCurveItem(message);
        if (isReal) {
            onRealTimeDataMessage(item);
        } else {
            onHistoryDataMessage(item);
        }
    }

    private void onHistoryDataMessage(CurveItem item) {
        if (historyPoints.getAndIncrement() == 0) {
            pointTimer = System.currentTimeMillis();
        }
        loadBuffer.add(item);
    }

    private void onRealTimeDataMessage(CurveItem item) {
        if (keyNotInRange(item.getKey())) {
            log.warn("Curve {} received point outside the allowed range: {}", info.getId(), StaticMapper.toJson(item));
            return;
        }

        if (lastSaved == null || Double.compare(item.getKey(), lastSaved.getKey()) > 0) {
            if (!loadBuffer.isEmpty() && Double.compare(item.getKey(), loadBuffer.last().getKey()) < 0) {
                log.warn("Curve {} real time point {} precedes last history point {}", info.getId(), item, loadBuffer.last());
            }
            realItemCache.add(item);

            //Сохраняем точки реалтайм в БД, если кривая вся загружена
            if (loadStatus == LoadStatus.LOADED) saveCachedItems(true);
            updateInfo(item);
            sendWsMessage(new PointMessage(info.getId(), item.getKey(), item.getValue()));

        } else if (info.getClassWitsml().equals("SYNTHETIC")) {
            updateReloadData(item);
        }
    }

    public synchronized void onDataEndMessage(DataEndMessage message) {
        int sent = message.getSentCount();
        int received = historyPoints.getAndSet(0);

        log.debug("End msg for {}: {},first point {}, last point {}", getInfo().getId(), message, loadBuffer.isEmpty() ? null : loadBuffer.first(), loadBuffer.isEmpty() ? null : loadBuffer.last());

        if (sent == 0) {
            realItemCache.addAll(historyItemCache);

            //------------ Удаляем дубликаты точек, которые могли прийти в реалтайм во время загрузки (бывает такое) ------------
            AtomicDouble from = new AtomicDouble(-1);
            AtomicDouble to = new AtomicDouble(-1);
            AtomicInteger count = new AtomicInteger(0);
            realItemCache.removeIf(item -> {
                boolean alreadySaved = lastSaved != null && compareItems(lastSaved, item) >= 0;
                if (alreadySaved) {
                    if (from.get() == -1 || compareItems(item, from.get()) < 0) {
                        from.set(item.getKey());
                    }
                    if (to.get() == -1 || compareItems(item, to.get()) > 0) {
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
            toggleLoadStatus(LoadStatus.LOADED);
            sendWsMessage(new LoadedMessage(info.getId()));
            log.info("Curve {} data loaded, {}", info.getId(), message);

            CurveStatusNotifier.notifyStatus(info.getId(), LoadStatus.LOADED);

        } else if (sent != received) {
            log.error("Curve {} received count {} not equals to sent {}", info.getId(), received, sent);
            addRequestJob();

        } else {
            if (received != loadBuffer.size()) {
                log.error("Buffer size {} not equals to received {}", loadBuffer.size(), received);
            }

            long millis = Math.max(1, System.currentTimeMillis() - pointTimer);
            long pointsPerSecond = received * 1000L / millis;
            log.info("Curve {} received {} items with avg speed {} points/sec. Request->firstPoint {} ms, firstPoint->lastPoint {} ms",
                    info.getId(), received, pointsPerSecond, pointTimer - requestTimer, millis);

            historyItemCache.addAll(loadBuffer);
            saveCachedItems(false);
            updateInfoFromBuffer();
            sendWsMessage(new PartMessage(info.getId(), loadBuffer.first().getKey(), loadBuffer.last().getKey()));
            addRequestJob();
        }
    }

    private void saveCachedItems(boolean isReal) {
        TreeSet<CurveItem> curveItems;
        if (isReal) curveItems = realItemCache;
        else curveItems = historyItemCache;

        CurveItem tmpLast = lastSaved;
        int tmpCount = 0;

        while (itemCacheIsFull(isReal)) {
            ArrayList<CurveItem> itemsBatch = new ArrayList<>(dispatcher.config.BATCH_SIZE);

            for (int i = 0; i < dispatcher.config.BATCH_SIZE; i++) {
                itemsBatch.add(curveItems.pollFirst());
            }

            dispatcher.repository.saveItems(List.of(ItemDto.fromItemsList(info.getId(), itemsBatch, isNumeric())));

            if (firstSaved == null) {
                firstSaved = itemsBatch.get(0);
            }
            tmpLast = itemsBatch.get(itemsBatch.size() - 1);
            tmpCount += itemsBatch.size();

            segmentProcessor.addItems(itemsBatch);
        }

        lastSaved = tmpLast;
        savedCount = savedCount + tmpCount;
    }

    public synchronized Set<Integer> getScaleSet() {
        return Set.copyOf(segmentProcessor.getSegmentCache().keySet());
    }

    public synchronized List<CurveItem> getTail(Double from, Double to) {
        List<CurveItem> result = new ArrayList<>();

        double finalFrom = from == null ? Double.MIN_VALUE : from;
        double finalTo = to == null ? Double.MAX_VALUE : to;

        historyItemCache.stream()
                .filter(item -> compareItems(item, finalFrom) >= 0 && compareItems(item, finalTo) <= 0)
                .forEach(result::add);


        realItemCache.stream()
                .filter(item -> compareItems(item, finalFrom) >= 0 && compareItems(item, finalTo) <= 0)
                .forEach(result::add);

        return result;
    }

    private String reloadLog(CurveItem received) {
        Map<String, String> data = new LinkedHashMap<>();
        data.put("curve id", String.valueOf(info.getId()));
        data.put("mnemonic", info.getMnemonic());
        data.put("received point", received.toString());
        data.put("last point in db", lastSaved.toString());
        data.put("first point in memory", realItemCache.isEmpty() ? "null" : realItemCache.first().toString());
        data.put("last point in memory", realItemCache.isEmpty() ? "null" : realItemCache.last().toString());
        data.put("received - last in db", (received.getKey() - lastSaved.getKey()) + " ms");
        data.put("received - first in memory", realItemCache.isEmpty() ? "null" : (received.getKey() - realItemCache.first().getKey()) + " ms");
        data.put("received - last in memory", realItemCache.isEmpty() ? "null" : (received.getKey() - realItemCache.last().getKey()) + " ms");
        data.put("points in memory", String.valueOf(realItemCache.size()));
        return data.toString();
    }

    private void updateReloadData(CurveItem item) {
        Double from = item.getKey();
        boolean skipLogging = false;

        if (reloadData == null) {
            log.info("Curve set reload time in 3 minutes. Details: {}", reloadLog(item));
            reloadData = new ReloadData();
            toggleLoadStatus(LoadStatus.BLOCKED);
            reloadData.reloadTime = LocalDateTime.now().plusMinutes(3);
        } else if (System.currentTimeMillis() - lastUpdateReloadLogTime > 60000) {
            log.info("Curve reload time extended to {}. {} more old points suppressed. Details: {}", reloadData.reloadTime, suppressedOldPoints, reloadLog(item));
            lastUpdateReloadLogTime = System.currentTimeMillis();
            suppressedOldPoints = 0;
        } else {
            suppressedOldPoints++;
            skipLogging = true;
        }

        reloadData.from = reloadData.from != null && Double.compare(reloadData.from, from) < 0 ? reloadData.from : from;
        dispatcher.getRequestManager().removeLoadTask(info.getId(), skipLogging);
    }

    protected synchronized void reload() {
        if (reloadData != null) {
            boolean reloadTimeHasCome = reloadData.reloadTime.isBefore(LocalDateTime.now());
            if (reloadTimeHasCome && loadStatus != LoadStatus.IN_PROGRESS) {
                log.info("Curve {} will now be reloaded from {}", info.getId(), reloadData.from);
                clearData(reloadData.from);
                segmentProcessor.restoreScaledSegments();
                reloadData = null;
                addRequestJob();
            } else if (System.currentTimeMillis() - lastBlockedLogTime > 60000) {
                String reason =
                        reloadTimeHasCome
                                ? "history load is active (buffered size = " + loadBuffer.size() + ")"
                                : "reload time has not come yet (expected at " + reloadData.reloadTime + ")";
                log.info("Curve {} can't reload because {}", info.getId(), reason);

                lastBlockedLogTime = System.currentTimeMillis();
            }
        }
    }

    private void clearData(Double from) {
        loadBuffer.clear();
        historyItemCache.clear();
        realItemCache.clear();
        segmentProcessor.getSegmentCache().clear();

        if (lastSaved != null && compareItems(lastSaved, reloadData.from) >= 0) {
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

        info.setMinValue(dispatcher.repository.getItemsMinValue(info.getId()));
        info.setMaxValue(dispatcher.repository.getItemsMaxValue(info.getId()));
    }

    private boolean itemCacheIsFull(boolean isReal) {
        if (isReal) {
            return realItemCache.size() >= dispatcher.config.BATCH_SIZE + dispatcher.config.MARGIN_SIZE;
        }
        return historyItemCache.size() >= dispatcher.config.BATCH_SIZE;
    }

    private boolean isNumeric() {
        return info.getAxisDefinition() == null && (info.getTypeLogData() == LogDataType.DOUBLE || info.getTypeLogData() == LogDataType.LONG);
    }

    private void updateInfoKeys(CurveItem min, CurveItem max) {
        if (info.getMinKey() == null || Double.compare(info.getMinKey(), min.getKey()) > 0) {
            info.setMinKey(min.getKey());
        }

        if (info.getMaxKey() == null || Double.compare(info.getMaxKey(), max.getKey()) <= 0) {
            info.setMaxKey(max.getKey());
            info.setLastValue(String.valueOf(max.getValue()));
        }

        if (info.getMaxLoadedKey() == null || Double.compare(info.getMaxLoadedKey(), max.getKey()) <= 0) {
            info.setMaxLoadedKey(max.getKey());
        }

        if (info.getMinLoadedKey() == null || Double.compare(info.getMinLoadedKey(), min.getKey()) > 0) {
            info.setMinLoadedKey(min.getKey());
        }
    }

    private void updateInfo(CurveItem item) {
        updateInfoKeys(item, item);

        updateMaxMinValue(List.of(item));
    }

    private void updateInfoFromBuffer() {
        updateInfoKeys(loadBuffer.first(), loadBuffer.last());

        updateMaxMinValue(loadBuffer);
        dispatcher.repository.saveOrUpdateInfo(info);
    }

    private void updateMaxMinValue(Collection<CurveItem> items) {
        if (isNumeric()) {
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

                    log.trace(e.getMessage(), e);
                }
            }
        }
    }

    private void addRequestJob() {
        loadBuffer.clear();
        RequestType requestType = fromRest ? RequestType.LOAD_REST : RequestType.LOAD_ACTIVE;
        if (isReload) {
            requestType = RequestType.RELOAD;
            isReload = false;
        }
        dispatcher.getRequestManager().addRequestTask(new RequestTask(info.getId(), requestType, this::doItemsRequest));
        toggleLoadStatus(LoadStatus.IN_QUEUE);
    }

    private synchronized void doItemsRequest() {
        String from = findFrom();
        String to = findTo();
        CurveDataRequest request = new CurveDataRequest(
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
                    if (loadStatus != LoadStatus.IN_QUEUE) {
                        log.warn("Curve {} status is {} when must be {}", info.getId(), loadStatus, LoadStatus.IN_QUEUE);
                    }
                    toggleLoadStatus(LoadStatus.IN_PROGRESS);
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
            addRequestJob();
            throw new NullResponseException();
        }
    }

    private int compareItems(CurveItem o1, CurveItem o2) {
        return compareItems(o1, o2.getKey());
    }

    private int compareItems(CurveItem o1, double o2) {
        return Double.compare(o1.getKey(), o2);
    }

    private CurveItem mapCurveItem(CurveDataMessage message) {
        var messageData = message.getData();
        var key = isDateTimeCurve ? messageData.getTime().toInstant().toEpochMilli() : messageData.getDepth();

        return new CurveItem(key, messageData.getValue());
    }

    private boolean keyNotInRange(Double key) {
        double minVal = getMinVal();
        double maxVal = getMaxVal();
        return Double.compare(key, minVal) < 0 || Double.compare(key, maxVal) > 0;
    }

    protected double getMaxVal() {
        return isDateTimeCurve ? OffsetDateTime.now().plusHours(24).toInstant().toEpochMilli() : dispatcher.config.MAX_DEPTH_METERS;
    }

    protected double getMinVal() {
        return isDateTimeCurve ? dispatcher.config.MIN_TIME_MILLIS : dispatcher.config.MIN_DEPTH_METERS;
    }

    private Double findFromDouble() {
        Double result;
        String logMessage;
        if (!historyItemCache.isEmpty()) {
            result = historyItemCache.last().getKey();
            logMessage = "last history item";
        } else if (lastSaved != null) {
            result = lastSaved.getKey();
            logMessage = "last saved item key";
        } else {
            result = getMinVal();
            logMessage = "default";
        }

        if (isDateTimeCurve) {
            result = (double) Instant.ofEpochMilli((long) result.doubleValue()).plusMillis(10).toEpochMilli();
        }
        log.debug("findFromDouble() for {}: {} key is {}", getInfo().getId(), logMessage, result);
        return result;
    }

    private String findFrom() {
        Double key = findFromDouble();

        return getKeyAsString(key, info.getIndexType());
    }

    private String findTo() {
        Double key = getMaxVal();

        return getKeyAsString(key, info.getIndexType());
    }

    protected String getKeyAsString(Double key, LogIndexType type) {
        if (key == null) {
            return null;
        }
        return isDateTimeCurve
                ? OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)
                : new BigDecimal(key).setScale(4, RoundingMode.UP).stripTrailingZeros().toPlainString(); // FIXME: зачем округление?
    }

    public void sendWsMessage(WsMessage message) {
        dispatcher.webSocketMessageProcessor.sendMessage(info.getId(), message);
    }

    public enum LoadStatus {
        IN_QUEUE, IN_PROGRESS, LOADED, UNKNOWN, BLOCKED
    }

    private static class ReloadData {
        private Double from;
        private LocalDateTime reloadTime;
    }
}
