package ru.geosteering.goperform.cache.model;

import lombok.*;
import ru.geosteering.commonModels.dataService.CurveInfo;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class ExtraCurveInfo extends CurveInfo {

    private Double minValue;
    private Double maxValue;

    public ExtraCurveInfo(CurveInfo info, Double minValue, Double maxValue) {
        super(info.getId(), info.getMnemonic(), info.getIndexType(), info.getUnit(),
                info.getValue(), info.getTimeMax(), info.getTimeMin(), info.getMdMax(), info.getMdMin(),
                info.isRigis(), info.getAxisDefinition(), info.getLastChanged(), info.getClassWitsml(), info.getTypeLogData());
        this.minValue = minValue;
        this.maxValue = maxValue;
    }
}
