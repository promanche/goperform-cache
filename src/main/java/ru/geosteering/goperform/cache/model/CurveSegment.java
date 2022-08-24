package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

@Getter
@Setter
@EqualsAndHashCode
public class CurveSegment {
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double firstKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double lastKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double minVal;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double maxVal;

    public void addItem(CurveItem item) {

        if (item != null) {
            Double value = (Double) item.getValue();

            if (maxVal == null || value > maxVal) {
                maxVal = value;
            }
            if (minVal == null || value < minVal) {
                minVal = value;
            }
            if (firstKey == null) {
                firstKey = item.getKey();
            }
            lastKey = item.getKey();
        }
    }
}
