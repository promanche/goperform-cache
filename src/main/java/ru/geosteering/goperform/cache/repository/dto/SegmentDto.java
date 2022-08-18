package ru.geosteering.goperform.cache.repository.dto;

import lombok.*;
import ru.geosteering.goperform.cache.model.CurveSegment;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class SegmentDto {

    private Long curveId;
    private Integer scale;
    private Double first;
    private Double last;
    private String data;

    public static SegmentDto fromLinesList(Long curveId, Integer scale, List<CurveSegment> lines) {
        SegmentDto segmentDto = new SegmentDto();
        segmentDto.setCurveId(curveId);
        segmentDto.setScale(scale);
        segmentDto.setFirst(lines.get(0).getFirstKey());
        segmentDto.setLast(lines.get(lines.size() - 1).getLastKey());
        segmentDto.setData(StaticMapper.toJson(lines));

        return segmentDto;
    }
}
