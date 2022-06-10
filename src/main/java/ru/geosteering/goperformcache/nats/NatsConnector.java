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
import java.util.concurrent.*;

import static ru.geosteering.goperformcache.config.Config.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class NatsConnector {

    private Connection connection;
    private CustomErrorListener errorListener;
    private final RealtimeService realtimeService;
    private final HistoryService historyService;

    @PostConstruct
    private void init() {
        historyService.setConnector(this);
        errorListener = new CustomErrorListener(this);
    }

    public void connect() {

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .connectionListener((connection, events) -> log.info("Nats connection {} status: {}", HOST, connection.getStatus()))
                .noReconnect()
                .errorListener(errorListener)
                .authHandler(Nats.credentials(CREDENTIALS_FILE))
                .server(HOST)
                .build();

        try {
            connection = Nats.connect(options);
        } catch (IOException | InterruptedException e) {
            log.error("Connection exception: {}", e.getMessage());
        }

        if (connection != null) {
            Dispatcher dispatcher = connection.createDispatcher();
            dispatcher.subscribe(SUBJECT + ".*", realtimeService);
            dispatcher.subscribe(SUBJECT + "." + HISTORY_NUID + ".*", historyService);
        }
    }

    public void sendRequest(byte[] data) throws ExecutionException, InterruptedException, TimeoutException {
        Message message = NatsMessage.builder()
                .subject(SUBJECT)
                .data(data)
                .build();

        Message response = connection.request(message).get(3, TimeUnit.SECONDS);
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
    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.error("Exception while closing: {}", e.getMessage());
            }
        }
    }

    void reconnect() {
        new Thread(() -> {
            try {
                onError();
                closeConnection();

                int seconds = RECONNECT_TIMEOUT_SECONDS;
                while (seconds > 0) {
                    log.info("Reconnect waiting... " + seconds);
                    Thread.sleep(1000);
                    seconds--;
                }

                connect();
                onReconnect();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }
}
