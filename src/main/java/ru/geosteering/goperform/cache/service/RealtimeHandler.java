package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.MapperUtils;

import javax.annotation.PreDestroy;
import java.util.concurrent.*;

@Service
@Slf4j
public class RealtimeHandler implements MessageHandler {

    private final Storage storage;
    private final SimpMessagingTemplate wsTemplate;
    private final Config config;
    private final DataLoader loader;
    private final MetaDataProcessor metaDataProcessor;

    private ExecutorService messageHandler;

    public RealtimeHandler(Storage storage, SimpMessagingTemplate wsTemplate, Config config, DataLoader loader, MetaDataProcessor metaDataProcessor) {
        this.storage = storage;
        this.wsTemplate = wsTemplate;
        this.config = config;
        this.loader = loader;
        this.metaDataProcessor = metaDataProcessor;
        messageHandler = Executors.newFixedThreadPool(config.REALTIME_THREADS);
    }

    public void restart() {
        messageHandler = Executors.newFixedThreadPool(config.REALTIME_THREADS);
    }

    @Override
    public void onMessage(Message msg) {
        messageHandler.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {
        try {
            if (notSpam(msg.getSubject())) {

                ApiMessage apiMessage = MapperUtils.parseObject(new String(msg.getData()), ApiMessage.class);

                if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.CURVE_DATA) {
                    CurveDataMessage curveDataMessage = (CurveDataMessage) apiMessage;
                    CurveItem item = CurveItem.fromCurveDataItem(curveDataMessage.getData());
                    metaDataProcessor.updateByNewItem(curveDataMessage.getId(), item);

                    if (notOld(curveDataMessage.getId(), item.getKey())) {
                        storage.add(curveDataMessage.getId(), item, true);
                        String toWs = "{\"id\":" + curveDataMessage.getId() + ",\"point\":" + MapperUtils.toJson(item) + "}";
                        wsTemplate.convertAndSend("/curve/" + curveDataMessage.getId() + "/new-point", toWs);

                    } else {
                        loader.addForReload(curveDataMessage.getId(), item.getKey(), 60 * 5);
                    }
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private boolean notSpam(String subject) {
        String tail = subject.substring(config.SUBJECT.length() + 1);
        try {
            Long.parseLong(tail);
            return true;
        } catch (NumberFormatException e) {
            log.trace("Spam detected. Subject: {}", subject);
            return false;
        }
    }

    private boolean notOld(Long id, Double key) {
        Double lastKey = metaDataProcessor.getMetaData(id).getLastDBKey();
        return lastKey == null || key > lastKey;
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
        } catch (InterruptedException e) {
            log.error(getClass().getSimpleName() + "messageHandler shutdown exception: {}", e.getMessage(), e);
        }

        storage.clearRealTimeData();
    }
}
