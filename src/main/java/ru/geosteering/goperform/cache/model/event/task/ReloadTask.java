package ru.geosteering.goperform.cache.model.event.task;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.event.Event;

import java.time.LocalDateTime;

@Getter
@ToString(callSuper = true)
public class ReloadTask extends Event {

    private final Long id;
    private final Double from;
    private final LocalDateTime reloadTime;

    public ReloadTask(Long id, Double from, LocalDateTime reloadTime) {
        super(EventType.RELOAD_TASK);
        this.id = id;
        this.from = from;
        this.reloadTime = reloadTime;
    }
}
