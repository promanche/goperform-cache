package ru.geosteering.goperform.cache.model.ws;

import lombok.*;

@Getter
@Setter
@ToString
@AllArgsConstructor
public abstract class WsMessage {

    private final MessageType type;
    private final Long id;

    public enum MessageType {
        POINT,
        PART,
        LOADED
    }
}
