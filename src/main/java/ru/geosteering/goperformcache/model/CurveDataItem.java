package ru.geosteering.goperformcache.model;

import lombok.*;

import java.time.OffsetDateTime;

@Getter
@Setter
@EqualsAndHashCode
@ToString
public class CurveDataItem {
    OffsetDateTime time;
    Double depth;
    Double value;
}
