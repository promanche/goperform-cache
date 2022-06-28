package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.CacheUtils;

import javax.annotation.PreDestroy;
import java.util.concurrent.*;

import static ru.geosteering.goperform.cache.config.Config.REALTIME_THREADS;
import static ru.geosteering.goperform.cache.config.Config.SUBJECT;

@Component
@Slf4j
public class RealtimeHandler implements MessageHandler {

    private final Storage storage;
    private final SimpMessagingTemplate wsTemplate;

    private ExecutorService messageHandler = Executors.newFixedThreadPool(REALTIME_THREADS);

    public RealtimeHandler(Storage storage, SimpMessagingTemplate wsTemplate) {
        this.storage = storage;
        this.wsTemplate = wsTemplate;
    }

    public void restart() {
        storage.onRestartReal();
        messageHandler = Executors.newFixedThreadPool(REALTIME_THREADS);
    }

    @Override
    public void onMessage(Message msg) {
        messageHandler.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {
        try {
            if (notSpam(msg.getSubject())) {

                ApiMessage apiMessage = CacheUtils.parseApiMessage(new String(msg.getData()), msg.getSubject());

                if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.CURVE_DATA) {
                    CurveDataMessage curveDataMessage = (CurveDataMessage) apiMessage;
                    storage.add(curveDataMessage.getId(), CacheItem.fromCurveDataItem(curveDataMessage.getData()), true);
                    wsTemplate.convertAndSend("/realtime/curve", curveDataMessage);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private boolean notSpam(String subject) {
        String tail = subject.substring(SUBJECT.length() + 1);
        try {
            Long.parseLong(tail);
            return true;
        } catch (NumberFormatException e) {
            log.warn("Spam detected. Subject: {}", subject);
            return false;
        }
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
    }
}
