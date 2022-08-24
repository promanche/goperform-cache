package ru.geosteering.goperform.cache.model.event.apimessage;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class DataEndMessage extends Event {

    private final Long id;
    private final Result result;

    public DataEndMessage(Long id, Result result) {
        super(EventType.DATA_END_MESSAGE);
        this.id = id;
        this.result = result;
    }

    public enum Result {
        DONE, PART, ERROR
    }
}
