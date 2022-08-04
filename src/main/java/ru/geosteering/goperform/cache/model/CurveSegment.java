package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode
public class CurveSegment {
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double firstKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double lastKey;
    private Double minVal;
    private Double maxVal;
}
