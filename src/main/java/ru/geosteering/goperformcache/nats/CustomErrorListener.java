package ru.geosteering.goperformcache.nats;

import io.nats.client.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class CustomErrorListener implements ErrorListener {

    private final NatsConnector connector;

    @Override
    public void errorOccurred(Connection conn, String error) {
        log.error("errorOccurred: {}", error);
    }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        log.error("exceptionOccurred: {}", exp.getMessage());
        connector.reconnect();
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        log.error("SlowConsumer detected on subject: {}", ((Subscription) consumer).getSubject());
    }
}
