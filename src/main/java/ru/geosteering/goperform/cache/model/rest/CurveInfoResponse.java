package ru.geosteering.goperform.cache.model.rest;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.*;

import java.util.List;
import java.util.Set;

@Getter
@Setter
@AllArgsConstructor
public class CurveInfoResponse {

    private Long id;
    private String mnemonic;
    private LogIndexType indexType;
    private String unit;
    private List<CsAxisDefinition> axisDefinition;
    private String classWitsml;
    private LogDataType typeLogData;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double maxValue;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double minValue;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double maxKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double minKey;
    private int saved;
    private Set<Integer> scaleSet;
    private Object lastValue;
}
