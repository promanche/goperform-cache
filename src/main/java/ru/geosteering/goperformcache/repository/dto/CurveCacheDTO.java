package ru.geosteering.goperformcache.repository.dto;

import lombok.*;
import ru.geosteering.goperformcache.model.CurveDataItem;
import ru.geosteering.goperformcache.utils.CacheUtils;

import java.time.OffsetDateTime;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class CurveCacheDTO {
    private Long curveId;
    private OffsetDateTime first;
    private OffsetDateTime last;
    private String cache;

    public CurveCacheDTO(Long curveId, List<CurveDataItem> list) {
        this.curveId = curveId;
        this.first = list.get(0).getTime();
        this.last = list.get(list.size() - 1).getTime();
        this.cache = CacheUtils.toJson(list);
    }
}
