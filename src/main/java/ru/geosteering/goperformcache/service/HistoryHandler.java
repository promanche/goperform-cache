package ru.geosteering.goperformcache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.CurveDataItem;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperformcache.storage.Storage;
import ru.geosteering.goperformcache.utils.CacheUtils;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.geosteering.goperformcache.config.Config.HISTORY_REQUEST_LIMIT;
import static ru.geosteering.goperformcache.config.Config.HISTORY_THREADS;

@Component
@Slf4j
public class HistoryHandler implements MessageHandler {

    private final Storage storage;
    private final HistoryLoader historyLoader;

    private final Map<Long, AtomicInteger> receivedCount;
    private final Map<Long, Set<CurveDataItem>> buffer;

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
                    case CURVE_DATA -> processCurveData((CurveDataMessage) apiMessage);
                    case DATA_END -> processDataEnd((DataEndMessage) apiMessage, msg.getSubject());
                    case STATUS -> processStatus((StatusMessage) apiMessage, msg.getSubject());
                    default -> log.warn("Some ApiMessage: {}, subject: {}", apiMessage, msg.getSubject());
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void processCurveData(CurveDataMessage curveDataMessage) {

        buffer.computeIfAbsent(curveDataMessage.getId(), v -> ConcurrentHashMap.newKeySet(HISTORY_REQUEST_LIMIT))
                .add(curveDataMessage.getData());

        receivedCount.computeIfAbsent(curveDataMessage.getId(), v -> new AtomicInteger(0))
                .incrementAndGet();
    }

    private void processDataEnd(DataEndMessage dataEndMessage, String subject) throws InterruptedException {

        Thread.sleep(100);

        Long id = CacheUtils.getIdFromSubject(subject);

        int sent = dataEndMessage.getSentCount();
        int received = receivedCount.containsKey(id) ? receivedCount.remove(id).get() : -1;

        if (sent == 0) {
            buffer.remove(id);
            historyLoader.applyStatus(id, LoadStatus.DONE);

        } else if (sent != received) {
            log.error("Received count '{}' not equals to sent '{}'", received, sent);
            buffer.get(id).clear();
            historyLoader.applyStatus(id, LoadStatus.ERROR);

        } else {
            log.info("History part received: id {}, message {}", id, dataEndMessage);
            drainToStorage(id);
            historyLoader.applyStatus(id, LoadStatus.PART);
        }

        historyLoader.onEndMessage();
    }

    private void processStatus(StatusMessage statusMessage, String subject) {
        log.warn("StatusMessage: {}, subject {}", statusMessage, subject);
    }

    private void drainToStorage(Long id) {
        log.info("Drain buffer to storage. Curve id: {}, items: {}", id, buffer.get(id).size());
        storage.addAll(id, buffer.get(id), false);
        buffer.get(id).clear();
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
