package ru.geosteering.goperform.cache.model.event.item;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.Event;

import java.util.List;

@Getter
@ToString(callSuper = true)
public class ItemsBatch extends Event {

    private final Long id;
    private final List<CurveItem> items;

    public ItemsBatch(Long id, List<CurveItem> items) {
        super(EventType.ITEMS_BATCH);
        this.id = id;
        this.items = items;
    }
}
