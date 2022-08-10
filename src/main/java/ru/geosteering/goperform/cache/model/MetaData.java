package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@NoArgsConstructor
public class MetaData extends CurveInfo {

    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double maxValue;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double minValue;
    private int itemsInDB;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double firstDBKey;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double lastDBKey;
    private Set<Integer> scaleSet = ConcurrentHashMap.newKeySet();

    public MetaData(CurveInfo info) {
        super(
                info.getId(),
                info.getMnemonic(),
                info.getIndexType(),
                info.getUnit(),
                info.getValue(),
                info.getTimeMax(),
                info.getTimeMin(),
                info.getMdMax(),
                info.getMdMin(),
                info.isRigis(),
                info.getAxisDefinition(),
                info.getLastChanged(),
                info.getClassWitsml(),
                info.getTypeLogData()
        );
    }
}
