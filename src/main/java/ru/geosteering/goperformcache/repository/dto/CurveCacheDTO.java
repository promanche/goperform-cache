package ru.geosteering.goperformcache.repository.dto;

import lombok.*;
import ru.geosteering.commonModels.dataService.CurveDataItem;
import ru.geosteering.goperformcache.utils.CacheUtils;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class CurveCacheDTO {
    private Long curveId;
    private LocalDateTime first;
    private LocalDateTime last;
    private String cache;

    public CurveCacheDTO(Long curveId, List<CurveDataItem> list) {
        this.curveId = curveId;
        this.first = list.get(0).time.toLocalDateTime();
        this.last = list.get(list.size() - 1).time.toLocalDateTime();
        this.cache = CacheUtils.toJson(list);
    }

    public List<CurveDataItem> toItems() {
        return CacheUtils.parseCurveDataItems(cache);
    }
}
