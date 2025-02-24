package ru.geosteering.goperform.cache.processor;

import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class DepthCurveProcessor extends SingleCurveProcessor {
    public DepthCurveProcessor(ExtraCurveInfo info, boolean fromRest, CurveDispatcher dispatcher) {
        super(info, fromRest, dispatcher);
    }

    @Override
    protected double getMaxVal() {
        return dispatcher.config.MAX_DEPTH_METERS;
    }

    @Override
    protected double getMinVal() {
        return dispatcher.config.MIN_DEPTH_METERS;
    }

    @Override
    protected String getKeyAsString(Double key, LogIndexType type) {
        if (key == null) {
            return null;
        }
        return new BigDecimal(key).setScale(4, RoundingMode.UP).stripTrailingZeros().toPlainString(); // FIXME: зачем округление?
    }
}
