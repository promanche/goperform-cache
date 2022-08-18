package ru.geosteering.goperform.cache.model.event;

public class CurveNotActive extends Event<Long> {

    public CurveNotActive(Long curveId) {
        super(EventType.CURVE_NOT_ACTIVE, curveId);
    }
}
