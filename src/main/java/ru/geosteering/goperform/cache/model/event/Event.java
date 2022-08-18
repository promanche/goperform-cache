package ru.geosteering.goperform.cache.model.event;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@ToString
public abstract class Event<T> {

    private EventType type;
    private T payload;


    public enum EventType {
        REALTIME_API_MESSAGE,
        HISTORY_API_MESSAGE,
        NATS_CONNECTION_STATUS,
        OLD_CURVE_ITEM,
        ITEMS_BATCH_COLLECTED,
        NEW_CURVE_ITEM,
        CURVE_NOT_ACTIVE,
        NEW_ACTIVE_CURVE,
        HISTORY_END_MESSAGE
    }
}
