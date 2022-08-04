package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.service.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
@RequiredArgsConstructor
public class NatsConnector {

    private final RealtimeHandler realtimeHandler;
    private final HistoryHandler historyHandler;
    private final HistoryLoader historyLoader;
    private final Config config;
    private final MetaDataProcessor metaDataProcessor;

    private Connection connection;
    private CustomErrorListener errorListener;

    @PostConstruct
    private void init() {
        errorListener = new CustomErrorListener(this);
        historyLoader.setConnector(this);
        metaDataProcessor.setConnector(this);

        historyLoader.start();
    }

    public void connect() {

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .connectionListener((connection, events) -> log.info("Nats connection {} status: {}", config.HOST, connection.getStatus()))
                .noReconnect()
                .errorListener(errorListener)
                .authHandler(Nats.credentials(config.CREDENTIALS_FILE))
                .server(config.HOST)
                .build();

        try {
            connection = Nats.connect(options);
        } catch (IOException | InterruptedException e) {
            log.error("Connection exception: {}", e.getMessage(), e);
        }

        if (connection != null) {
            Dispatcher dispatcher = connection.createDispatcher();
            dispatcher.subscribe(config.SUBJECT + ".*", realtimeHandler);
            dispatcher.subscribe(config.SUBJECT + "." + config.HISTORY_NUID + ".*", historyHandler);
        }
    }

    public Message sendRequest(byte[] data) throws ExecutionException, InterruptedException {
        Message message = NatsMessage.builder()
                .subject(config.SUBJECT)
                .data(data)
                .build();

        Message response = connection.request(message).get();
        log.info("Response: {}", new String(response.getData()));

        return response;
    }

    private void onError() {
        historyLoader.stop();
        realtimeHandler.stop();
        historyHandler.stop();
    }

    private void onReconnect() {
        realtimeHandler.restart();
        historyHandler.restart();
        historyLoader.restart();
    }

    @PreDestroy
    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("Exception while closing: {}", e.getMessage(), e);
            }
        }
    }

    void reconnect() {
        new Thread(() -> {
            try {
                onError();
                closeConnection();

                int seconds = config.RECONNECT_TIMEOUT_SECONDS;
                while (seconds > 0) {
                    log.info("Reconnect waiting... " + seconds);
                    Thread.sleep(1000);
                    seconds--;
                }

                connect();
                onReconnect();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }).start();
    }
}
