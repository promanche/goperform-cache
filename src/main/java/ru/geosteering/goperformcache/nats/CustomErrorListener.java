package ru.geosteering.goperformcache.nats;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomErrorListener implements ErrorListener {

    private final NatsConnector connector;

    public CustomErrorListener(NatsConnector connector) {
        this.connector = connector;
    }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        log.error("exceptionOccurred: {}", exp.getMessage(), exp);
        connector.reconnect();
    }
}
