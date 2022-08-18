package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.EventListener;
import ru.geosteering.goperform.cache.model.event.*;
import ru.geosteering.goperform.cache.service.EventBus;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.*;

@Component
@RequiredArgsConstructor
@Slf4j
public class HistoryMessageHandler implements MessageHandler, EventListener {

    private final Config config;

    private ExecutorService executor;

    @PostConstruct
    private void register() {
        EventBus.register(List.of(Event.EventType.NATS_CONNECTION_STATUS), this);
    }

    @Override
    public void onEvent(Event<?> event) {

        ConnectionListener.Events status = ((NatsConnectionStatus) event).getPayload();

        if (status == ConnectionListener.Events.CONNECTED) {
            start();
        }

        if (status == ConnectionListener.Events.DISCONNECTED || status == ConnectionListener.Events.CLOSED) {
            stop();
        }
    }

    @Override
    public void onMessage(Message msg) {
        executor.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {

        try {

            ApiMessage apiMessage = StaticMapper.parseObject(new String(msg.getData()), ApiMessage.class);

            if (apiMessage != null) {
                EventBus.post(new HistoryApiMessage(apiMessage, msg.getSubject()));
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void start() {
        executor = Executors.newFixedThreadPool(config.HISTORY_THREADS);
    }

    private void stop() {

        try {
            executor.shutdown();

            if (!executor.awaitTermination(1500, TimeUnit.MILLISECONDS)) {
                log.warn(getClass().getSimpleName() + " executor shutdown timeout");
                executor.shutdownNow();
            }

            log.info(getClass().getSimpleName() + " executor shutdown");

        } catch (InterruptedException e) {
            log.error(getClass().getSimpleName() + "executor shutdown exception: {}", e.getMessage(), e);
        }
    }
}
