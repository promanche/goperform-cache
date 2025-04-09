package ru.geosteering.goperform.cache.model.ws;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@AllArgsConstructor
public abstract class WsMessage {

    private final MessageType msgType;
    private final Long id;

    public enum MessageType {
        POINT,
        PART,
        LOADED,
        PROCESSOR_INIT
    }
}
