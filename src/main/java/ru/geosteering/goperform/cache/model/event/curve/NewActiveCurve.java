package ru.geosteering.goperform.cache.model.event.curve;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class NewActiveCurve extends Event {

    private final Long id;

    public NewActiveCurve(Long id) {
        super(EventType.NEW_ACTIVE_CURVE);
        this.id = id;
    }
}
