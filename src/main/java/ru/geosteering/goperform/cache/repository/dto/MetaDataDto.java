package ru.geosteering.goperform.cache.repository.dto;

import lombok.*;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.utils.StaticMapper;

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
        metaDataDto.setData(StaticMapper.toJson(metaData));

        return metaDataDto;
    }
}
