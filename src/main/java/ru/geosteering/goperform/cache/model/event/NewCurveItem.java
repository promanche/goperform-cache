package ru.geosteering.goperform.cache.model.event;

import lombok.Getter;
import ru.geosteering.goperform.cache.model.CurveItem;

@Getter
public class NewCurveItem extends Event<CurveItem> {

    private final Long curveId;

    public NewCurveItem(Long curveId, CurveItem payload) {
        super(EventType.NEW_CURVE_ITEM, payload);
        this.curveId = curveId;
    }
}
