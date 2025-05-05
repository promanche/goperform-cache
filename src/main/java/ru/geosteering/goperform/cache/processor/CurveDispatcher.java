package ru.geosteering.goperform.cache.processor;

import io.nats.client.Message;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.nats.ConnectionEventListener;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.processor.request.RequestManager;
import ru.geosteering.goperform.cache.processor.request.RequestTask;
import ru.geosteering.goperform.cache.processor.request.RequestType;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import static ru.geosteering.goperform.cache.processor.SingleCurveProcessor.LoadStatus;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Диспетчер обработчиков кривых.
 * <p>
 * Отвечает за управление обработчиками кривых ({@link SingleCurveProcessor}),
 * передачу данных в соответствующие обработчики и создание новых обработчиков при необходимости.
 * <p>
 * Также содержит планируемые задачи для:
 * - Сбора статистики.
 * - Очистки сломанных кривых.
 * - Удаления неактивных кривых и их обработчиков.
 */
@Slf4j
@Getter
@Component
@RequiredArgsConstructor
public class CurveDispatcher implements ConnectionEventListener {

    protected final Config config;
    protected final MainRepository repository;
    protected final WebSocketMessageProcessor webSocketMessageProcessor;
    private final RequestManager requestManager;
    private final CurveStateManager curveStateManager;
    private final StatisticCollector statisticCollector;
    private final Map<Long, SingleCurveProcessor> processors = new ConcurrentHashMap<>();
    private final Map<Long, LocalDateTime> brokenCurves = new ConcurrentHashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Периодический сбор статистики.
     */
    @Scheduled(fixedDelayString = "#{@config.STATISTIC_PERIOD_SECONDS * 1000}")
    public void collectStatistics() {
        try {
            statisticCollector.collect(processors, brokenCurves);
        } catch (Exception e) {
            log.error("Error while collecting statistics", e);
        }
    }

    /**
     * Периодическая перезагрузка обработчиков.
     */
    @Scheduled(fixedDelay = 5, timeUnit = TimeUnit.SECONDS)
    public void reloadProcessors() {
        try {
            processors.values().forEach(SingleCurveProcessor::reload);
        } catch (Exception e) {
            log.error("Error while reloading processors", e);
        }
    }

    /**
     * Очищает сломанные кривые.
     */
    @Scheduled(fixedDelay = 3, initialDelay = 3, timeUnit = TimeUnit.MINUTES)
    public void clearBroken() {
        try {
            log.info("Clearing broken curves older than 30 minutes...");
            brokenCurves.entrySet().removeIf(entry ->
                    entry.getValue().plusMinutes(30).isBefore(LocalDateTime.now()));
        } catch (Exception e) {
            log.error("Error while clearing broken curves", e);
        }
    }

    /**
     * Удаляет неактивные кривые и их обработчики.
     */
    @Scheduled(fixedDelay = 12, initialDelay = 3, timeUnit = TimeUnit.HOURS)
    public void deleteInactiveCurves() {
        log.info("Deleting inactive curves and processors...");
        curveStateManager.getExpiredCurves(config.DAYS_UNTIL_CURVE_IS_REMOVED)
                .forEach(this::deleteCurve);
        curveStateManager.getExpiredCurves(config.DAYS_UNTIL_CURVE_PROCESSOR_IS_REMOVED)
                .forEach(this::removeProcessor);
    }

    /**
     * Вызывается при подключении к NATS.
     */
    @Override
    public void onConnect() {
        log.info("NATS connection established. Initializing components...");
        requestManager.onConnect();
        processors.values().forEach(SingleCurveProcessor::onConnect);
    }

    /**
     * Вызывается при отключении от NATS.
     */
    @Override
    public void onDisconnect() {
        log.warn("NATS connection lost. Resetting components...");
        requestManager.onDisconnect();
        processors.values().forEach(SingleCurveProcessor::onDisconnect);
    }

    /**
     * Обрабатывает сообщение с Gostream.
     *
     * @param message Сообщение с Gostream.
     * @param isReal  Флаг, указывающий, является ли точка реального времени.
     */
    public void onCurveDataMessage(CurveDataMessage message, boolean isReal) {
        Long curveId = message.getId();

        if (isBroken(curveId)) {
            log.warn("Curve {} is marked as broken. Skipping processing.", curveId);
            return;
        }

        SingleCurveProcessor curveProcessor = getCurveProcessor(curveId, false);
        if (curveProcessor != null) {
            curveProcessor.onCurveDataMessage(message, isReal);
            statisticCollector.onCurveDataMessage(message, isReal);
        } else {
            log.warn("No processor found for curve ID: {}. Data message skipped.", curveId);
        }

    }

    /**
     * Обрабатывает сообщение о завершении отправления исторических данных кривой.
     *
     * @param message Сообщение о завершении данных.
     * @param subject Топик.
     */
    public void onDataEndMessage(DataEndMessage message, String subject) {
        try {
            var idOpt = parseId(subject);
            if (idOpt.isEmpty()) {
                log.error("Failed to parse ID from subject: {}", subject);
                return;
            }

            Long id = idOpt.get();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(300));
            SingleCurveProcessor curveProcessor = getCurveProcessor(id, false);
            if (curveProcessor != null) {
                curveProcessor.onDataEndMessage(message);
            } else {
                log.warn("No processor found for curve ID: {}. Data end message skipped.", id);
            }
        } catch (Exception e) {
            log.error("Error while processing data end message for subject: {}", subject, e);
        } finally {
            requestManager.onDataEndMessage();
        }
    }

    /**
     * Обрабатывает ошибку запроса данных.
     *
     * @param curveId ID кривой.
     */
    public void onErrorDataRequest(Long curveId) {
        if (!brokenCurves.containsKey(curveId)) {
            LocalDateTime now = LocalDateTime.now();
            brokenCurves.put(curveId, now);
            log.warn("Curve {} marked as broken at {} for 30 minutes", curveId, now);
        } else {
            log.debug("Curve {} is already marked as broken", curveId);
        }

        SingleCurveProcessor removed = processors.remove(curveId);
        if (removed != null) {
            log.warn("Removed processor for curve ID: {}", curveId);
        } else {
            log.debug("No processor found for curve ID: {}", curveId);
        }
    }

    /**
     * Полностью перезагружает кэш кривой.
     *
     * @param curveId ID кривой.
     */
    public void fullCurveReload(Long curveId) {
        log.info("Performing full reload for curve ID: {}", curveId);
        deleteCurve(curveId);
        requestCurveInfo(curveId, true);
        curveStateManager.changed(curveId);
    }

    /**
     * Удаляет кривую по ID.
     *
     * @param curveId ID кривой.
     */
    public void deleteCurve(Long curveId) {
        log.info("Deleting all curve data: {}", curveId);
        deleteCurveDataFromDB(curveId);
        removeProcessor(curveId);
        curveStateManager.removeState(curveId);
    }

    /**
     * Проверяет, является ли кривая сломанной.
     *
     * @param curveId ID кривой.
     * @return true, если кривая сломана; иначе false.
     */
    public boolean isBroken(Long curveId) {
        return brokenCurves.containsKey(curveId);
    }

    /**
     * Проверяет наличие обработчика для кривой.
     *
     * @param curveId ID кривой.
     * @return true, если обработчик существует; иначе false.
     */
    public boolean isCurveProcessorPresent(Long curveId) {
        return processors.containsKey(curveId);
    }

    /**
     * Возвращает обработчик кривой или создает новый, если его нет.
     * Если процессор отсутствует, инициирует асинхронную загрузку данных и публикует статус LOADING в общий топик NATS.
     * После завершения загрузки публикуется статус LOADED.
     *
     * @param curveId  ID кривой.
     * @param fromRest Флаг, указывающий, был ли запрос инициирован через REST.
     * @return Обработчик кривой или null, если создание невозможно.
     */
    public SingleCurveProcessor getCurveProcessor(Long curveId, boolean fromRest) {
        return processors.computeIfAbsent(curveId, key -> {
            log.info("Creating new processor for curve ID: {}, fromRest: {}", curveId, fromRest);
            SingleCurveProcessor processor = createSingleCurveProcessor(key, fromRest);
            if (processor == null) {
                requestCurveInfo(curveId, fromRest);
                try {
                    CurveStatusMessage statusMsg = new CurveStatusMessage(curveId, LoadStatus.IN_QUEUE);
                    String message = objectMapper.writeValueAsString(statusMsg);
                    String subject = "curve.status";
                    NatsConnector.publish(subject, message.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    log.error("Failed to serialize CurveStatusMessage for curveId {}: {}", curveId, e.getMessage(), e);
                }
            }
            curveStateManager.changed(curveId);
            return processor;
        });
    }

    /**
     * Создает запрос на получение информации о кривой и инициирует асинхронную загрузку.
     * Используется для реализации request-reply паттерна: после старта загрузки публикуется LOADING, после завершения — LOADED.
     *
     * @param curveId  ID кривой.
     * @param fromRest Флаг, указывающий, был ли запрос инициирован через REST.
     */
    private void requestCurveInfo(Long curveId, boolean fromRest) {
        RequestType type = fromRest ? RequestType.INFO_REST : RequestType.INFO_ACTIVE;
        log.info("Requesting curve info for ID: {}, type: {}", curveId, type);
        requestManager.addRequestTask(new RequestTask(curveId, type, () -> doInfoRequest(curveId, fromRest)));
    }

    /**
     * Создает новый обработчик кривой.
     *
     * @param curveId  ID кривой.
     * @param fromRest Флаг, указывающий, был ли запрос инициирован через REST.
     * @return Новый обработчик кривой или null, если информация о кривой недоступна.
     */
    private SingleCurveProcessor createSingleCurveProcessor(Long curveId, boolean fromRest) {
        return repository.getInfo(curveId)
                .map(info -> {
                    log.info("Initializing processor for curve ID: {}, fromRest: {}", curveId, fromRest);
                    return new SingleCurveProcessor(info, fromRest, this);
                })
                .orElseGet(() -> {
                    log.warn("Failed to initialize processor for curve ID: {}. Info not found.", curveId);
                    return null;
                });
    }

    /**
     * Выполняет запрос информации о кривой и публикует статус LOADED в общий топик после завершения загрузки.
     *
     * @param curveId  ID кривой.
     * @param fromRest Флаг, указывающий, был ли запрос инициирован через REST.
     */
    private void doInfoRequest(Long curveId, boolean fromRest) {
        try {
            CurveDataRequest request = new CurveDataRequest();
            request.setCurveId(curveId);
            request.setInfoOnly(true);
            request.setWithRange(true);

            log.info("Sending info request for curve ID: {}, payload: {}", curveId, request);
            Message response = NatsConnector.sendRequest(config.SUBJECT, StaticMapper.toBytes(request));

            if (response == null) {
                log.error("No response received for info request of curve ID: {}", curveId);
                return;
            }

            log.info("Received response for curve ID: {}, payload: {}", curveId, new String(response.getData()));
            ApiMessage apiMessage = StaticMapper.parseObject(new String(response.getData()), ApiMessage.class);

            switch (Objects.requireNonNull(apiMessage).getType()) {
                case CURVE_INFO -> {
                    CurveInfo curveInfo = ((CurveInfoMessage) apiMessage).getCurveInfo();
                    ExtraCurveInfo info = new ExtraCurveInfo(validateCurveInfo(curveInfo));
                    repository.saveOrUpdateInfo(info);
                    processors.computeIfAbsent(curveInfo.getId(), k -> {
                        log.info("Created new processor for curve ID: {} after info request", k);
                        // Публикуем статус LOADED в общий топик
                        try {
                            CurveStatusMessage statusMsg = new CurveStatusMessage(k, LoadStatus.LOADED);
                            String message = objectMapper.writeValueAsString(statusMsg);
                            String subject = "curve.status";
                            NatsConnector.publish(subject, message.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                            log.error("Failed to serialize CurveStatusMessage for curveId {}: {}", k, e.getMessage(), e);
                        }
                        return new SingleCurveProcessor(info, fromRest, this);
                    });
                }
                case STATUS -> {
                    StatusMessage statusMessage = (StatusMessage) apiMessage;
                    if (statusMessage.getStatus() != EResult.OK) {
                        log.error("Missing curve info for ID: {}, status message: {}", curveId, statusMessage);
                        brokenCurves.put(curveId, LocalDateTime.now());
                    }
                }
                default ->
                        log.error("Unknown response type for curve ID: {}, payload: {}", curveId, new String(response.getData()));
            }
        } catch (Exception e) {
            log.error("Error while processing info request for curve ID: {}", curveId, e);
        }
    }

    /**
     * Валидирует информацию о кривой.
     *
     * @param info Информация о кривой.
     * @return Обновленная информация о кривой.
     */
    private CurveInfo validateCurveInfo(CurveInfo info) {
        validateDepth(info);
        validateTime(info);
        return info;
    }

    /**
     * Валидирует значения глубины.
     *
     * @param info Информация о кривой.
     */
    private void validateDepth(CurveInfo info) {
        Double mdMin = info.getMdMin();
        Double mdMax = info.getMdMax();

        if (mdMin != null && (mdMin < config.MIN_DEPTH_METERS || mdMin > config.MAX_DEPTH_METERS)) {
            log.warn("Invalid MdMin value for curve: {}. Resetting to null.", info);
            info.setMdMin(null);
        }

        if (mdMax != null && (mdMax < config.MIN_DEPTH_METERS || mdMax > config.MAX_DEPTH_METERS)) {
            log.warn("Invalid MdMax value for curve: {}. Resetting to null.", info);
            info.setMdMax(null);
        }
    }

    /**
     * Валидирует значения времени.
     *
     * @param info Информация о кривой.
     */
    private void validateTime(CurveInfo info) {
        long maxAllowedMillis = OffsetDateTime.now().plusHours(24).toInstant().toEpochMilli();

        if (info.getTimeMin() != null) {
            long timeMinMillis = info.getTimeMin().toInstant().toEpochMilli();
            if (timeMinMillis < config.MIN_TIME_MILLIS || timeMinMillis > maxAllowedMillis) {
                log.warn("Invalid timeMin value for curve: {}. Resetting to null.", info);
                info.setTimeMin(null);
            }
        }

        if (info.getTimeMax() != null) {
            long timeMaxMillis = info.getTimeMax().toInstant().toEpochMilli();
            if (timeMaxMillis < config.MIN_TIME_MILLIS || timeMaxMillis > maxAllowedMillis) {
                log.warn("Invalid timeMax value for curve: {}. Resetting to null.", info);
                info.setTimeMax(null);
            }
        }
    }

    /**
     * Парсит ID из топика.
     *
     * @param subject Топик.
     * @return ID кривой или null, если парсинг не удался.
     */
    private Optional<Long> parseId(String subject) {
        try {
            String[] parts = subject.split("\\.");
            return Optional.of(Long.parseLong(parts[parts.length - 1]));
        } catch (Exception e) {
            log.error("Failed to parse ID from subject: {}", subject, e);
            return Optional.empty();
        }
    }

    /**
     * Удаляет информацию о кривой из репозитория.
     *
     * @param curveId ID кривой.
     */
    private void deleteCurveDataFromDB(Long curveId) {
        repository.deleteInfo(curveId);

        boolean isSegmentsExist = repository.getSegmentsRecordsCount(curveId) > 0;
        boolean isItemsExist = repository.getItemsRecordsCount(curveId) > 0;

        while (isSegmentsExist || isItemsExist) {
            if (isItemsExist) {
                isItemsExist = repository.deleteBatchItems(curveId);
            }
            if (isSegmentsExist) {
                isSegmentsExist = repository.deleteBatchSegments(curveId);
            }
        }

        log.info("Curve ID: {} deleted successfully", curveId);
    }

    /**
     * Удаляет обработчик кривой и запрос на загрузку.
     *
     * @param curveId ID кривой.
     */
    private void removeProcessor(Long curveId) {
        requestManager.removeLoadTask(curveId, true);
        processors.remove(curveId);
        log.info("Removed processor for curve ID: {}", curveId);
    }

    // Вспомогательный класс для статуса
    class CurveStatusMessage {
        public Long curveId;
        public LoadStatus status;
        public CurveStatusMessage(Long curveId, LoadStatus status) {
            this.curveId = curveId;
            this.status = status;
        }
    }
}