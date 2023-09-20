package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
public class CurveSegment {
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double firstKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double lastKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double minVal;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double maxVal;

    public CurveSegment(CurveItem item, int scale) {
        int msOnPixel = scale * 60000 / 120;
        long key = item.getKey().longValue();
        firstKey = (double) (key - key % msOnPixel);
        lastKey = firstKey + msOnPixel;
    }

    public void addItem(CurveItem item) {

        if (item != null) {
            double value = ((Number) item.getValue()).doubleValue();

            if (maxVal == null || value > maxVal) {
                maxVal = value;
            }
            if (minVal == null || value < minVal) {
                minVal = value;
            }
        }
    }
}
