package ru.geosteering.goperformcache.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CurveDataMessage {
    String type;
    Long id;
    CurveDataItem data;
}
