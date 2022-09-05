package ru.geosteering.goperform.cache.model.ws;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
public class LoadedMessage extends WsMessage {

    public LoadedMessage(Long id) {
        super(MessageType.LOADED, id);
    }
}
