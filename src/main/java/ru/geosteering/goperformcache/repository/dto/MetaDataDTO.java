package ru.geosteering.goperformcache.repository.dto;

import lombok.*;

import java.time.OffsetDateTime;

@Getter
@Setter
@ToString
public class MetaDataDTO {
    long curveId;
    OffsetDateTime first;
    OffsetDateTime last;
}
