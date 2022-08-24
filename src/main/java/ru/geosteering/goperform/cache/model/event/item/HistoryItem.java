package ru.geosteering.goperform.cache.model.event.item;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class HistoryItem extends Event {

    private final Long id;
    private final CurveItem curveItem;

    public HistoryItem(Long id, CurveItem curveItem) {
        super(EventType.HISTORY_ITEM);
        this.id = id;
        this.curveItem = curveItem;
    }
}
