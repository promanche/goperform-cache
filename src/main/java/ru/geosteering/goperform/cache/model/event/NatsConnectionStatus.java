package ru.geosteering.goperform.cache.model.event;

import io.nats.client.ConnectionListener;

public class NatsConnectionStatus extends Event<ConnectionListener.Events> {

    public NatsConnectionStatus(ConnectionListener.Events status) {
        super(EventType.NATS_CONNECTION_STATUS, status);
    }
}
