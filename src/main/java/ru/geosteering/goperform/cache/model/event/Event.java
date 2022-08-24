package ru.geosteering.goperform.cache.model.event;

import lombok.*;

@Getter
@AllArgsConstructor
@ToString
public abstract class Event {

    private EventType type;

    public enum EventType {
        REALTIME_API_MESSAGE,
        HISTORY_API_MESSAGE,
        NATS_CONNECTION_STATUS,
        ITEMS_BATCH,
        REALTIME_ITEM,
        HISTORY_ITEM,
        NEW_ACTIVE_CURVE,
        DATA_END_MESSAGE,
        CLEAR_TASK,
        LOAD_TASK,
        RELOAD_TASK
    }
}
