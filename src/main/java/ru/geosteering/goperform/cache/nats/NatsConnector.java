package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import io.nats.client.impl.ErrorListenerLoggerImpl;
import io.nats.client.impl.NatsMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
@RequiredArgsConstructor
public class NatsConnector {

    private final RealtimeMessageHandler realtimeHandler;
    private final HistoryMessageHandler historyHandler;
    private final ConnectionEventDispatcher connectionEventDispatcher;
    private final Config config;

    private static Connection connection;

    public void connect() {

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .connectionListener
                        ((conn, status) -> new Thread(() -> {
                                    log.info("Nats connection status: {}", status.name());

                                    if (status == ConnectionListener.Events.DISCONNECTED) {
                                        realtimeHandler.waitTerminated();
                                        historyHandler.waitTerminated();
                                        connectionEventDispatcher.onDisconnect();
                                        reconnect();
                                    }

                                    if (status == ConnectionListener.Events.CONNECTED) {
                                        connectionEventDispatcher.onConnect();
                                    }
                                }).start()
                        )
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

        if (isConnected()) {
            Dispatcher dispatcher = connection.createDispatcher();
            dispatcher.subscribe(config.SUBJECT + ".*", realtimeHandler);
            dispatcher.subscribe(config.SUBJECT + "." + config.HISTORY_NUID + ".*", historyHandler);
        }
    }

    private static boolean isConnected() {
        return connection != null && connection.getStatus() == Connection.Status.CONNECTED;
    }

    public static Message sendRequest(String subject, byte[] data) {

        Message response = null;

        if (isConnected()) {
            try {
                Message request = NatsMessage.builder()
                        .subject(subject)
                        .data(data)
                        .build();

                log.info("Request: {}", new String(data));

                response = connection.request(request).get();

                log.debug("Response: {}", new String(response.getData()));

            } catch (InterruptedException | ExecutionException e) {
                log.error("Send request exception: {}", e.getMessage(), e);
            }
        }

        return response;
    }

    @PreDestroy
    private void closeConnection() {
        try {
            connection.close();
        } catch (Exception e) {
            log.error("Exception while closing connection: {}", e.getMessage(), e);
        }
    }

    void reconnect() {
        try {
            closeConnection();

            int seconds = config.RECONNECT_TIMEOUT_SECONDS;
            while (seconds > 0) {
                log.info("Reconnect waiting... " + seconds);
                Thread.sleep(1000);
                seconds--;
            }

            realtimeHandler.initExecutor();
            historyHandler.initExecutor();
            connect();

        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }
}
