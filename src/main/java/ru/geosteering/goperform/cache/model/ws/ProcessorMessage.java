package ru.geosteering.goperform.cache.model.ws;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class ProcessorMessage extends WsMessage {

    public ProcessorMessage(Long id) {
        super(MessageType.PROCESSOR_INIT, id);
    }
}
