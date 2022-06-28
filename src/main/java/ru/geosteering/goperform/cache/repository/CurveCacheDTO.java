package ru.geosteering.goperform.cache.repository;

import lombok.*;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.model.ItemType;
import ru.geosteering.goperform.cache.utils.CacheUtils;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class CurveCacheDTO {
    private Long curveId;
    private ItemType type;
    private Double first;
    private Double last;
    private String cache;

    public CurveCacheDTO(Long curveId, List<CacheItem> list) {
        this.curveId = curveId;
        this.type = list.get(0).getType();
        this.first = list.get(0).getKey();
        this.last = list.get(list.size() - 1).getKey();
        this.cache = CacheUtils.toJson(list);
    }

    public List<CacheItem> toItems() {
        return CacheUtils.parseCacheItems(cache);
    }
}
