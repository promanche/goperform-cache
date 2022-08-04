package ru.geosteering.goperform.cache.repository;

import lombok.*;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.utils.MapperUtils;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class MetaDataDto {

    private Long curveId;
    private String data;

    public static MetaDataDto fromMetaData(MetaData metaData) {
        MetaDataDto metaDataDto = new MetaDataDto();
        metaDataDto.setCurveId(metaData.getId());
        metaDataDto.setData(MapperUtils.toJson(metaData));

        return metaDataDto;
    }
}
