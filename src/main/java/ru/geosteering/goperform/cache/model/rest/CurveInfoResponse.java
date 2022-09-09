package ru.geosteering.goperform.cache.model.rest;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.Setter;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.*;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
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
    private Set<Integer> scaleSet = ConcurrentHashMap.newKeySet();
    private Object lastValue;

    public static CurveInfoResponse fromMetaData(MetaData data) {

        CurveInfoResponse response = new CurveInfoResponse();

        response.setId(data.getId());
        response.setMnemonic(data.getMnemonic());
        response.setIndexType(data.getIndexType());
        response.setUnit(data.getUnit());
        response.setAxisDefinition(data.getAxisDefinition());
        response.setClassWitsml(data.getClassWitsml());
        response.setTypeLogData(data.getTypeLogData());
        response.setMaxValue(data.getMaxValue());
        response.setMinValue(data.getMinValue());
        response.setMaxKey(data.getMaxKey());
        response.setMinKey(data.getMinKey());
        response.setSaved(data.getItemsInDB());
        response.setScaleSet(data.getScaleSet());
        response.setLastValue(data.getLastValue());

        return response;
    }
}
