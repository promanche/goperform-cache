package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.MapperUtils;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class HistoryHandler implements MessageHandler {

    private final Storage storage;
    private final DataLoader dataLoader;
    private final Map<Long, AtomicInteger> receivedCount;
    private final Map<Long, Set<CurveItem>> buffer;
    private final Config config;
    private final MetaDataProcessor metaDataProcessor;
    private ExecutorService messageHandler;

    public HistoryHandler(Storage storage, DataLoader dataLoader, Config config, MetaDataProcessor metaDataProcessor) {
        this.storage = storage;
        this.dataLoader = dataLoader;
        this.config = config;
        this.metaDataProcessor = metaDataProcessor;
        receivedCount = new ConcurrentHashMap<>();
        buffer = new ConcurrentHashMap<>();
        messageHandler = Executors.newFixedThreadPool(config.HISTORY_THREADS);
    }

    public void restart() {
        receivedCount.clear();
        buffer.clear();
        messageHandler = Executors.newFixedThreadPool(config.HISTORY_THREADS);
    }

    @Override
    public void onMessage(Message msg) {
        messageHandler.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {

        try {
            ApiMessage apiMessage = MapperUtils.parseObject(new String(msg.getData()), ApiMessage.class);

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

        CurveItem item = CurveItem.fromCurveDataItem(curveDataMessage.getData());

        buffer.computeIfAbsent(curveDataMessage.getId(), v -> ConcurrentHashMap.newKeySet(config.HISTORY_REQUEST_LIMIT))
                .add(item);

        receivedCount.computeIfAbsent(curveDataMessage.getId(), v -> new AtomicInteger(0))
                .incrementAndGet();

        metaDataProcessor.updateByNewItem(curveDataMessage.getId(), item);
    }

    private void processDataEnd(DataEndMessage dataEndMessage, String subject) {

        Long id = parseId(subject);

        try {
            Thread.sleep(100);

            int sent = dataEndMessage.getSentCount();
            int received = receivedCount.containsKey(id) ? receivedCount.remove(id).get() : -1;

            if (sent == 0) {
                log.info("Data loaded: id {}, message {}", id, dataEndMessage);
                dataLoader.onLoadFull(id);

            } else if (sent != received) {
                log.error("Received count '{}' not equals to sent '{}'", received, sent);
                dataLoader.onLoadError(id);

            } else {
                log.info("History part received: id {}, message {}", id, dataEndMessage);
                drainToStorage(id);
                dataLoader.onLoadPart(id);
            }

            buffer.remove(id);

        } catch (Exception e) {
            log.error("DataEndMessage processing exception: {}", e.getMessage(), e);
            dataLoader.onLoadError(id);

        } finally {
            dataLoader.onEndMessage();
        }
    }

    private Long parseId(String subject) {

        try {
            String[] arr = subject.split("\\.");
            return Long.parseLong(arr[arr.length - 1]);
        } catch (Exception e) {
            log.error("Parsing id from subject exception: {}", subject, e);
            return null;
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

        storage.clearHistoryData();
    }
}
