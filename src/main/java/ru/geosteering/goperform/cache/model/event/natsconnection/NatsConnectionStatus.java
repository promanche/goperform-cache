package ru.geosteering.goperform.cache.model.event.natsconnection;

import io.nats.client.ConnectionListener;
import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class NatsConnectionStatus extends Event {

    private final ConnectionListener.Events status;

    public NatsConnectionStatus(ConnectionListener.Events status) {
        super(EventType.NATS_CONNECTION_STATUS);
        this.status = status;
    }
}
