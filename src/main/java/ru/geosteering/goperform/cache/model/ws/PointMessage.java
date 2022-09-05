package ru.geosteering.goperform.cache.model.ws;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

@Getter
@Setter
@ToString(callSuper = true)
public class PointMessage extends WsMessage {

    @JsonSerialize(using = CustomDoubleSerializer.class)
    private final Double key;
    private final Object value;

    public PointMessage(Long id, Double key, Object value) {
        super(MessageType.POINT, id);
        this.key = key;
        this.value = value;
    }
}
