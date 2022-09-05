package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.commonModels.dataService.AbstractDataItem;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

@Setter
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class CurveItem {

    @JsonSerialize(using = CustomDoubleSerializer.class)
    private final Double key;
    private final Object value;

    public static CurveItem fromAbstractDataItem(AbstractDataItem dataItem, boolean isDateTimeCurve) {

        Double key = isDateTimeCurve ? dataItem.getTime().toInstant().toEpochMilli() : dataItem.getDepth();

        return new CurveItem(key, dataItem.getValue());
    }
}
