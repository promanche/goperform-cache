package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.commonModels.dataService.AbstractDataItem;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

@Setter
@Getter
@EqualsAndHashCode
public class CurveItem {

    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double key;
    private Object value;

    public static CurveItem fromAbstractDataItem(AbstractDataItem dataItem, boolean isDateTimeCurve) {
        CurveItem curveItem = new CurveItem();
        curveItem.key = isDateTimeCurve ? dataItem.getTime().toInstant().toEpochMilli() : dataItem.getDepth();
        curveItem.value = dataItem.getValue();
        return curveItem;
    }
}
