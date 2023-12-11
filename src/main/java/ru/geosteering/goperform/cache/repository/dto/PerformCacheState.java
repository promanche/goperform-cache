package ru.geosteering.goperform.cache.repository.dto;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.geosteering.commonModels.webService.JSTreeResponse;

import java.time.OffsetDateTime;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class PerformCacheState {

    private long curveId;
    private OffsetDateTime state;
    private String wellId;

    public PerformCacheState(long curveId, OffsetDateTime state) {
        this.curveId = curveId;
        this.state = state;
    }

    public PerformCacheState(long curveId, OffsetDateTime state, JSTreeResponse well) {
        this.curveId = curveId;
        this.state = state;
        this.wellId = well.getId();
    }
}
