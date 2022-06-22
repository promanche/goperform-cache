package ru.geosteering.goperform.cache.controller.dto;

import lombok.Getter;
import lombok.Setter;
import ru.geosteering.commonModels.dataService.CurveDataItem;

import java.util.List;

@Getter
@Setter
public class CacheResponse {
    Long id;
    List<CurveDataItem> data;
}
