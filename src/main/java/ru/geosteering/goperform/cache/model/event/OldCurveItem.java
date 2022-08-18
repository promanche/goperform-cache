package ru.geosteering.goperform.cache.model.event;

import lombok.Getter;
import ru.geosteering.goperform.cache.model.CurveItem;

@Getter
public class OldCurveItem extends Event<CurveItem>{

    private final Long curveId;

    public OldCurveItem(Long curveId, CurveItem item) {
        super(EventType.OLD_CURVE_ITEM, item);
        this.curveId = curveId;
    }
}
