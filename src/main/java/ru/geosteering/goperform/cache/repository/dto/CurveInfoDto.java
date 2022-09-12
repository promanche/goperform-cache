package ru.geosteering.goperform.cache.repository.dto;

import lombok.*;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.utils.StaticMapper;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class CurveInfoDto {

    private Long id;
    private String data;

    public static CurveInfoDto fromCurveInfo(ExtraCurveInfo info) {
        CurveInfoDto curveInfoDto = new CurveInfoDto();
        curveInfoDto.setId(info.getId());
        curveInfoDto.setData(StaticMapper.toJson(info));

        return curveInfoDto;
    }
}
