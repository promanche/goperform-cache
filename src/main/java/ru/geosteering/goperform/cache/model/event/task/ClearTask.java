package ru.geosteering.goperform.cache.model.event.task;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class ClearTask extends Event {

    private final Long id;

    public ClearTask(Long id) {
        super(EventType.CLEAR_TASK);
        this.id = id;
    }
}
