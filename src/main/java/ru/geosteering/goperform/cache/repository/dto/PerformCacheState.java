package ru.geosteering.goperform.cache.repository.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class PerformCacheState {

    private long curveId;
    private OffsetDateTime updatedAt;
    private String wellId;

    public PerformCacheState(long curveId, OffsetDateTime updatedAt) {
        this.curveId = curveId;
        this.updatedAt = updatedAt;
    }

    public PerformCacheState(long curveId, OffsetDateTime updatedAt, String wellId) {
        this.curveId = curveId;
        this.updatedAt = updatedAt;
        this.wellId = wellId;
    }
}
