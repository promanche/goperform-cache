package ru.geosteering.goperform.cache.model.ws;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

@Getter
@Setter
@ToString(callSuper = true)
public class PartMessage extends WsMessage {

    @JsonSerialize(using = CustomDoubleSerializer.class)
    private final Double from;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private final Double to;

    public PartMessage(Long id, Double from, Double to) {
        super(MessageType.PART, id);
        this.from = from;
        this.to = to;
    }
}
