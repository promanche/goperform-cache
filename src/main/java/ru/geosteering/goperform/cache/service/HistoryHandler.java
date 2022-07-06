package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.CacheUtils;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.geosteering.goperform.cache.config.Config.HISTORY_REQUEST_LIMIT;
import static ru.geosteering.goperform.cache.config.Config.HISTORY_THREADS;

@Service
@Slf4j
public class HistoryHandler implements MessageHandler {

    private final Storage storage;
    private final HistoryLoader historyLoader;
    private final Map<Long, AtomicInteger> receivedCount;
    private final Map<Long, Set<CacheItem>> buffer;

    private ExecutorService messageHandler = Executors.newFixedThreadPool(HISTORY_THREADS);

    public HistoryHandler(Storage storage, HistoryLoader historyLoader) {
        this.storage = storage;
        this.historyLoader = historyLoader;
        receivedCount = new ConcurrentHashMap<>();
        buffer = new ConcurrentHashMap<>();
    }

    public void restart() {
        receivedCount.clear();
        buffer.clear();
        storage.onRestartHistory();
        messageHandler = Executors.newFixedThreadPool(HISTORY_THREADS);
    }

    @Override
    public void onMessage(Message msg) {
        messageHandler.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {

        try {
            ApiMessage apiMessage = CacheUtils.parseApiMessage(new String(msg.getData()), msg.getSubject());

            if (apiMessage != null) {
                ApiMessage.MessageType type = apiMessage.getType();

                switch (type) {
                    case CURVE_DATA:
                        processCurveData((CurveDataMessage) apiMessage);
                        break;
                    case DATA_END:
                        processDataEnd((DataEndMessage) apiMessage, msg.getSubject());
                        break;
                    case STATUS:
                        processStatus((StatusMessage) apiMessage, msg.getSubject());
                        break;
                    default:
                        log.warn("Some ApiMessage: {}, subject: {}", apiMessage, msg.getSubject());
                        break;
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void processCurveData(CurveDataMessage curveDataMessage) {

        buffer.computeIfAbsent(curveDataMessage.getId(), v -> ConcurrentHashMap.newKeySet(HISTORY_REQUEST_LIMIT))
                .add(CacheItem.fromCurveDataItem(curveDataMessage.getData()));

        receivedCount.computeIfAbsent(curveDataMessage.getId(), v -> new AtomicInteger(0))
                .incrementAndGet();
    }

    private void processDataEnd(DataEndMessage dataEndMessage, String subject) {

        Long id = CacheUtils.getIdFromSubject(subject);

        try {
            Thread.sleep(100);

            int sent = dataEndMessage.getSentCount();
            int received = receivedCount.containsKey(id) ? receivedCount.remove(id).get() : -1;

            if (sent == 0) {
                historyLoader.applyStatus(id, LoadStatus.DONE);

            } else if (sent != received) {
                log.error("Received count '{}' not equals to sent '{}'", received, sent);
                historyLoader.applyStatus(id, LoadStatus.ERROR);

            } else {
                log.info("History part received: id {}, message {}", id, dataEndMessage);
                drainToStorage(id);
                historyLoader.applyStatus(id, LoadStatus.PART);
            }

            buffer.remove(id);

        } catch (Exception e) {
            log.error("DataEndMessage processing exception: {}", e.getMessage(), e);
            historyLoader.applyStatus(id, LoadStatus.ERROR);

        } finally {
            historyLoader.onEndMessage();
        }
    }

    private void processStatus(StatusMessage statusMessage, String subject) {
        log.warn("StatusMessage: {}, subject {}", statusMessage, subject);
    }

    private void drainToStorage(Long id) {
        log.info("Drain buffer to storage. Curve id: {}, items: {}", id, buffer.get(id).size());
        storage.addAll(id, buffer.get(id), false);
    }

    @PreDestroy
    public void stop() {
        try {
            messageHandler.shutdown();
            if (!messageHandler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn(getClass().getSimpleName() + " messageHandler shutdown timeout");
                messageHandler.shutdownNow();
            }
            log.info(getClass().getSimpleName() + " messageHandler shutdown");
        } catch (Exception e) {
            log.error(getClass().getSimpleName() + "messageHandler shutdown exception: {}", e.getMessage(), e);
        }
    }
}
