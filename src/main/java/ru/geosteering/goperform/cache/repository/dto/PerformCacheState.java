package ru.geosteering.goperform.cache.repository.dto;

import lombok.*;

import java.time.OffsetDateTime;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class PerformCacheState {

    private long id;
    private OffsetDateTime updatedAt;
    private String wellId;

    public PerformCacheState(long curveId, OffsetDateTime updatedAt) {
        this.id = curveId;
        this.updatedAt = updatedAt;
    }

    public PerformCacheState(long curveId, OffsetDateTime updatedAt, String wellId) {
        this.id = curveId;
        this.updatedAt = updatedAt;
        this.wellId = wellId;
    }
}
