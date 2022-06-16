package ru.geosteering.goperformcache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperformcache.model.CurveDataItem;
import ru.geosteering.goperformcache.model.CurveDataMessage;
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
            String json = new String(msg.getData());

            CurveDataMessage curveDataMessage = CacheUtils.parseCurveDataMessage(json, msg.getSubject());

            if (curveDataMessage == null) {
                Long id = CacheUtils.getIdFromSubject(msg.getSubject());
                if (id == null) {
                    return;
                }

                if (CacheUtils.getFieldFromJson(json, "type").equalsIgnoreCase("end")) {
                    String sent = CacheUtils.getFieldFromJson(json, "sentCount");

                    int received = receivedCount.containsKey(id) ? receivedCount.remove(id).get() : 0;

                    if (sent.equals("0")) {
                        historyLoader.applyStatus(id, LoadStatus.DONE);
                        buffer.remove(id);

                    } else if (!sent.equals(String.valueOf(received))) {
                        log.error("Received count '{}' not equals to sent '{}'", received, sent);
                        buffer.get(id).clear();
                        historyLoader.applyStatus(id, LoadStatus.ERROR);

                    } else {
                        try {
                            log.info("History part received: subject {}, message {}", msg.getSubject(), json);
                            drainToStorage(id);
                            historyLoader.refreshMetaData(id);
                            historyLoader.applyStatus(id, LoadStatus.WAIT);
                        } catch (Exception e) {
                            //Это костыль для time = null
                            log.error(e.getMessage());
                            historyLoader.applyStatus(id, LoadStatus.ERROR);
                        } finally {
                            buffer.get(id).clear();
                        }
                    }

                    historyLoader.onEndMessage();
                }

            } else {

                buffer.computeIfAbsent(curveDataMessage.getId(), v -> ConcurrentHashMap.newKeySet(HISTORY_REQUEST_LIMIT))
                        .add(curveDataMessage.getData());

                receivedCount.computeIfAbsent(curveDataMessage.getId(), v -> new AtomicInteger(0))
                        .incrementAndGet();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
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
