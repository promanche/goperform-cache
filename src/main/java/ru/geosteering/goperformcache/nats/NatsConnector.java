package ru.geosteering.goperformcache.nats;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperformcache.service.HistoryService;
import ru.geosteering.goperformcache.service.RealtimeService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static ru.geosteering.goperformcache.config.Config.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class NatsConnector {

    private Connection connection;
    private CustomErrorListener listener;
    private final RealtimeService realtimeService;
    private final HistoryService historyService;

    @PostConstruct
    private void init() throws IOException, InterruptedException {
        historyService.setConnector(this);
        listener = new CustomErrorListener(this);
        connect();
    }

    private void connect() throws IOException, InterruptedException {

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .connectionListener((connection, events) -> log.info("Nats connection {} status: {}", HOST, connection.getStatus()))
                .noReconnect()
                .errorListener(new CustomErrorListener(this))
                .authHandler(Nats.credentials(CREDENTIALS_FILE))
                .server(HOST)
                .build();

        connection = Nats.connect(options);

        Dispatcher dispatcher = connection.createDispatcher();
        dispatcher.subscribe(SUBJECT + ".*", realtimeService);
        dispatcher.subscribe(SUBJECT + "." + HISTORY_NUID + ".*", historyService);
    }

    public void sendRequest(byte[] data) throws ExecutionException, InterruptedException {
        Message message = NatsMessage.builder()
                .subject(SUBJECT)
                .data(data)
                .build();

        Message response = connection.request(message).get();
        log.info("Response: {}", response.toString());
    }

    private void onError() {
        historyService.stop();
        realtimeService.stop();
    }

    private void onReconnect() {
        realtimeService.restart();
        historyService.restart();
    }

    @PreDestroy
    private void closeConnection() throws InterruptedException {
        if (connection != null) {
            connection.close();
        }
    }

    void reconnect() {
        new Thread(() -> {
            try {
                onError();
                closeConnection();

                while (true) {
                    int seconds = RECONNECT_TIMEOUT_SECONDS;
                    while (seconds > 0) {
                        log.info("Reconnect waiting... " + seconds);
                        Thread.sleep(1000);
                        seconds--;
                    }

                    connect();
                    if (connection.getStatus() == Connection.Status.CONNECTED) {
                        break;
                    } else {
                        log.warn("Not connected. Next try");
                    }
                }

                onReconnect();
            } catch (InterruptedException | IOException e) {
                log.error("Reconnect error: {}", e.getMessage(), e);
                log.warn("Shutdown");
                System.exit(0);
            }
        }).start();
    }
}
