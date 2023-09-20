package ru.geosteering.goperform.cache.model;

import lombok.*;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.*;

import java.util.List;

@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
public class ExtraCurveInfo {

    private long id;
    private String mnemonic;
    private LogIndexType indexType;
    private String unit;
    private List<CsAxisDefinition> axisDefinition;
    private String classWitsml;
    private LogDataType typeLogData;
    private Double minKey;
    private Double maxKey;
    private Double minValue;
    private Double maxValue;
    private Object lastValue;
    private Double minLoadedKey;
    private Double maxLoadedKey;

    public ExtraCurveInfo(CurveInfo info) {
        id = info.getId();
        mnemonic = info.getMnemonic();
        indexType = info.getIndexType();
        unit = info.getUnit();
        axisDefinition = info.getAxisDefinition();
        classWitsml = info.getClassWitsml();
        typeLogData = info.getTypeLogData();

        if (indexType == LogIndexType.MEASURED_DEPTH) {
            minKey = info.getMdMin() == null ? null : info.getMdMin();
            maxKey = info.getMdMax() == null ? null : info.getMdMax();
        } else {
            minKey = info.getTimeMin() == null ? null : (double) info.getTimeMin().toInstant().toEpochMilli();
            maxKey = info.getTimeMax() == null ? null : (double) info.getTimeMax().toInstant().toEpochMilli();
        }

        lastValue = info.getValue();
    }
}
