package ru.geosteering.goperform.cache.processor.request;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.metrics.MetricName;
import ru.geosteering.goperform.cache.metrics.MetricService;
import ru.geosteering.goperform.cache.nats.NatsConnector;

import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Менеджер запросов в NATS.
 * <p>
 * Содержит очередь задач ({@link #requestQueue}) для асинхронных запросов в NATS и контролирует количество одновременно выполняемых запросов.
 * Количество одновременных запросов ограничено значением {@link Config#NATS_ONETIME_REQUESTS}.
 * <p>
 * Запросы обрабатываются асинхронно с приоритетом. Очередь задач обрабатывается методом {@link #doRequestJob()}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RequestManager {

    private final Config config;
    private final MetricService metricService;

    /**
     * Имеет место на подумать и заменить на существующие синхронизированные очереди например: {@link PriorityBlockingQueue})
     */
    private final SynchronizedRequestQueue requestQueue = new SynchronizedRequestQueue();
    private final AtomicInteger requestAllowed = new AtomicInteger();


    /**
     * Обрабатывает задачи из очереди запросов.
     * <p>
     * Если соединение с NATS установлено и есть доступные слоты для запросов,
     * задача извлекается из очереди и выполняется.
     * После завершения задачи счетчик разрешенных запросов увеличивается.
     */

    @Scheduled(fixedRate = 30)
    public void doRequestJob() {
        if (NatsConnector.isConnected()) {
            if (requestAllowed.getAndDecrement() > 0 && !requestQueue.isEmpty()) {
                try {
                    metricService.decrementGauge(MetricName.REQUEST_ALLOWED);
                    RequestTask task = requestQueue.poll();
                    if (task != null) {
                        log.debug("Processing request task {} for {}", task.getType(), task.getCurveId());
                        task.getRequestJob().doRequest();
                    }
                } catch (Exception e) {
                    log.error("Error while processing request job", e);
                } finally {
                    metricService.incrementGauge(MetricName.REQUEST_ALLOWED);
                    requestAllowed.incrementAndGet();
                }
            } else {
                requestAllowed.incrementAndGet();
            }
        }
    }

    /**
     * Возвращает текущее количество разрешенных запросов.
     *
     * @return Текущее количество разрешенных запросов.
     */
    public Integer getRequestAllowed() {
        return requestAllowed.get();
    }

    /**
     * Добавляет новую задачу в очередь запросов.
     *
     * @param newTask Новая задача для выполнения.
     */
    public void addRequestTask(RequestTask newTask) {
        log.debug("Adding request task {} for {}", newTask.getType(), newTask.getCurveId());
        requestQueue.add(newTask);
    }

    /**
     * Удаляет задачи из очереди по ID кривой.
     *
     * @param curveId     ID кривой для удаления задачи.
     * @param skipLogging Флаг для пропуска логирования операции удаления.
     */
    public void removeLoadTask(Long curveId, boolean skipLogging) {
        if (!skipLogging) {
            log.debug("Removing load task for {} (if any)", curveId);
        }
        requestQueue.removeIf(task -> Objects.equals(task.getCurveId(), curveId)
                && (task.getType() == RequestType.LOAD_ACTIVE || task.getType() == RequestType.LOAD_REST));
    }

    /**
     * Очищает очередь запросов.
     */
    private void clearRequestQueue() {
        log.debug("Clearing request queue");
        requestQueue.clear();
    }

    /**
     * Устанавливает начальное количество разрешенных запросов при подключении к NATS.
     */
    public void onConnect() {
        int initialRequestsAllowed = config.NATS_ONETIME_REQUESTS;
        requestAllowed.set(initialRequestsAllowed);
        metricService.setGaugeValue(MetricName.REQUEST_ALLOWED, initialRequestsAllowed);
        log.info("Set initial allowed requests to {}", initialRequestsAllowed);
    }

    /**
     * Сбрасывает количество разрешенных запросов и очищает очередь при отключении от NATS.
     */
    public void onDisconnect() {
        clearRequestQueue();
        requestAllowed.set(0);
        metricService.setGaugeValue(MetricName.REQUEST_ALLOWED, 0);
        log.info("Disconnected from NATS. Cleared request queue and reset allowed requests to 0.");
    }

    /**
     * Увеличивает счетчик разрешенных запросов при получении сообщения об окончании данных.
     */
    public void onDataEndMessage() {
        requestAllowed.incrementAndGet();
        metricService.incrementGauge(MetricName.REQUEST_ALLOWED);
        log.debug("Incremented allowed requests count after receiving data end message.");
    }
}