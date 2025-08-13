package ru.geosteering.goperform.cache.model.rest;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import ru.geosteering.goperform.cache.processor.SingleCurveProcessor;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.CsAxisDefinition;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.util.List;
import java.util.Set;

@Getter
@Setter
@Builder
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
    private SingleCurveProcessor.LoadStatus status;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double minLoadedKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double maxLoadedKey;
    private boolean waitReload;
    private Boolean initializing;
    private String error;
}
