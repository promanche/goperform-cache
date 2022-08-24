package ru.geosteering.goperform.cache.model.event.task;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class LoadITask extends Event {

    private final Long id;

    public LoadITask(Long id) {
        super(EventType.LOAD_TASK);
        this.id = id;
    }
}
