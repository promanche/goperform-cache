package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class NatsConnector {

    private final RealtimeMessageHandler realtimeHandler;
    private final HistoryMessageHandler historyHandler;
    private final ConnectionEventDispatcher connectionEventDispatcher;
    private final Config config;
    private final ScheduledExecutorService connectionScheduler = Executors.newSingleThreadScheduledExecutor();

    private long lastConnectionTry = System.currentTimeMillis();

    private static Connection connection;

    @EventListener(ApplicationStartedEvent.class)
    public void initConnectionScheduler() {
        connectionScheduler.scheduleWithFixedDelay(() -> {
            if (!isConnected()) {
                int timeout = (int) ((System.currentTimeMillis() - lastConnectionTry) / 1000);
                if (timeout >= config.RECONNECT_TIMEOUT_SECONDS) {
                    log.info("Connecting...");
                    lastConnectionTry = System.currentTimeMillis();
                    connect();
                } else {
                    log.info("Connecting wait..." + (config.RECONNECT_TIMEOUT_SECONDS - timeout));
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void connect() {

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .connectionListener
                        ((conn, status) -> new Thread(() -> {
                                    log.info("Nats connection status: {}", status.name());

                                    if (status == ConnectionListener.Events.DISCONNECTED) {
                                        connectionEventDispatcher.onDisconnect();
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

    private class ErrorListenerLoggerImpl extends io.nats.client.impl.ErrorListenerLoggerImpl {
        @Override
        public void exceptionOccurred(final Connection conn, final Exception exp) {
            log.error("NATS exception occurred", exp);
        }
    }

    public static boolean isConnected() {
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

                response = connection.request(request).get();

            } catch (Exception e) {
                log.error("Send request exception: {}", e.getMessage(), e);
            }
        }

        return response;
    }

    public static void publish(String subject, byte[] data) {
        if (isConnected()) {
            try {
                connection.publish(subject, data);
            } catch (Exception e) {
                log.error("Publish exception: {}", e.getMessage(), e);
            }
        }
    }

    @PreDestroy
    private void closeConnection() {
        try {
            connection.close();
        } catch (Exception e) {
            log.error("Exception while closing connection: {}", e.getMessage(), e);
        }
    }
}
