package ru.geosteering.goperformcache.controller.dto;

import lombok.Getter;
import lombok.Setter;
import ru.geosteering.goperformcache.model.CurveDataItem;

import java.util.List;

@Getter
@Setter
public class CacheResponse {
    Long id;
    List<CurveDataItem> data;
}
