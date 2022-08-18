package ru.geosteering.goperform.cache.model.event;

public class NewActiveCurve extends Event<Long>{

    public NewActiveCurve(Long curveId) {
        super(EventType.NEW_ACTIVE_CURVE, curveId);
    }
}
