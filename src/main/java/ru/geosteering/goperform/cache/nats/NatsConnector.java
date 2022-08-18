package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import io.nats.client.impl.ErrorListenerLoggerImpl;
import io.nats.client.impl.NatsMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.event.NatsConnectionStatus;
import ru.geosteering.goperform.cache.service.*;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
@RequiredArgsConstructor
public class NatsConnector {

    private final RealtimeHandler realtimeHandler;
    private final HistoryHandler historyHandler;
    private final DataLoader dataLoader;
    private final Config config;

    private static Connection connection;

    public void connect() {

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .connectionListener((conn, status) -> {
                    log.info("Nats connection status: {}", status.name());
                    EventBus.post(new NatsConnectionStatus(status));
                    if (status == ConnectionListener.Events.CLOSED || status == ConnectionListener.Events.DISCONNECTED) {
                        reconnect();
                    }
                })
                .noReconnect()
                .errorListener(new ErrorListenerLoggerImpl())
                .authHandler(Nats.credentials(config.CREDENTIALS_FILE))
                .server(config.HOST)
                .build();

        try {
            connection = Nats.connect(options);
        } catch (IOException | InterruptedException e) {
            log.error("Connection exception: {}", e.getMessage(), e);
        }

        if (connection != null && connection.getStatus() == Connection.Status.CONNECTED) {
            Dispatcher dispatcher = connection.createDispatcher();
            dispatcher.subscribe(config.SUBJECT + ".*", realtimeHandler);
            dispatcher.subscribe(config.SUBJECT + "." + config.HISTORY_NUID + ".*", historyHandler);

            dataLoader.start();
        }
    }

    public static Message sendRequest(String subject, byte[] data) {

        Message response = null;

        if (connection != null && connection.getStatus() == Connection.Status.CONNECTED) {

            try {

                Message request = NatsMessage.builder()
                        .subject(subject)
                        .data(data)
                        .build();

                log.info("Request: {}", new String(data));

                response = connection.request(request).get();

                log.info("Response: {}", new String(response.getData()));

            } catch (InterruptedException | ExecutionException e) {
                log.error("Send request exception: {}", e.getMessage(), e);
            }
        }

        return response;
    }

    private void onError() {
        dataLoader.stop();
        realtimeHandler.stop();
        historyHandler.stop();
    }

    private void onReconnect() {
        realtimeHandler.restart();
        historyHandler.restart();
        dataLoader.restart();
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
