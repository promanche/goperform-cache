package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.commonModels.dataService.AbstractDataItem;

@Setter
@Getter
@EqualsAndHashCode
public class CurveItem {

    @JsonSerialize(using = CustomDoubleSerializer.class)
    Double key;
    Object value;

    public static CurveItem fromCurveDataItem(AbstractDataItem dataItem) {
        CurveItem curveItem = new CurveItem();
        curveItem.key = dataItem.getTime() == null ? dataItem.getDepth() : dataItem.getTime().toInstant().toEpochMilli();
        curveItem.value = dataItem.getValue();
        return curveItem;
    }
}
