package ru.geosteering.goperform.cache.model;

import lombok.*;
import ru.geosteering.commonModels.dataService.CurveInfo;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@NoArgsConstructor
public class MetaData extends CurveInfo {

    private Double maxValue;
    private Double minValue;
    private int itemsInDB;
    private Double firstDBKey;
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
