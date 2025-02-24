package ru.geosteering.goperform.cache.processor;

import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateTimeCurveProcessor extends SingleCurveProcessor {
    public DateTimeCurveProcessor(ExtraCurveInfo info, boolean fromRest, CurveDispatcher dispatcher) {
        super(info, fromRest, dispatcher);
    }

    protected double getMaxVal() {
        return OffsetDateTime.now().plusHours(24).toInstant().toEpochMilli();
    }

    protected double getMinVal() {
        return dispatcher.config.MIN_TIME_MILLIS;
    }

    @Override
    protected String getKeyAsString(Double key, LogIndexType type) {
        if (key == null) {
            return null;
        }

        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
    }
}
