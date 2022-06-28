package ru.geosteering.goperform.cache.controller;

import lombok.Getter;
import lombok.Setter;
import ru.geosteering.goperform.cache.model.CacheItem;

import java.util.List;

@Getter
@Setter
public class CacheResponse {
    private Long id;
    private List<CacheItem> data;
}
