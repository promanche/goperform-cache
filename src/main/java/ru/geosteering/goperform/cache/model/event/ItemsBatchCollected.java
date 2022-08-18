package ru.geosteering.goperform.cache.model.event;

import lombok.Getter;
import ru.geosteering.goperform.cache.model.CurveItem;

import java.util.List;

@Getter
public class ItemsBatchCollected extends Event<List<CurveItem>> {

    private final Long curveId;

    public ItemsBatchCollected(Long curveId, List<CurveItem> items) {
        super(EventType.ITEMS_BATCH_COLLECTED, items);
        this.curveId = curveId;
    }
}
