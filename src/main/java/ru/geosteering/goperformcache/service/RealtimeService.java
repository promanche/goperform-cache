package ru.geosteering.goperformcache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import ru.geosteering.goperformcache.model.CurveDataMessage;
import ru.geosteering.goperformcache.storage.Storage;
import ru.geosteering.goperformcache.utils.CacheUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.*;

import static ru.geosteering.goperformcache.config.Config.REALTIME_THREADS;

@Component
@Slf4j
public class RealtimeService implements MessageHandler {

    private final Storage storage;
    private final SimpMessagingTemplate wsTemplate;

    private ExecutorService messageHandler;

    public RealtimeService(Storage storage, SimpMessagingTemplate wsTemplate) {
        this.storage = storage;
        this.wsTemplate = wsTemplate;
    }

    @PostConstruct
    private void start() {
        messageHandler = Executors.newFixedThreadPool(REALTIME_THREADS);
        log.info(getClass().getSimpleName() + " started");
    }

    public void restart() {
        storage.onRestartReal();
        start();
    }

    @Override
    public void onMessage(Message msg) {
        messageHandler.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {

        CurveDataMessage curveDataMessage = CacheUtils.parseCurveDataMessage(new String(msg.getData()), msg.getSubject());

        if (curveDataMessage != null) {
            storage.add(curveDataMessage.getId(), curveDataMessage.getData(), true);
            wsTemplate.convertAndSend("/realtime/curve", curveDataMessage);
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
