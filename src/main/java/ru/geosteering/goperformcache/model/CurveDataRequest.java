package ru.geosteering.goperformcache.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import ru.geosteering.goperformcache.utils.CacheUtils;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class CurveDataRequest {
    private Long curveId;
    private String from;
    private String to;
    private Integer ms;
    private boolean infoOnly;
    private boolean withRange;
    private Integer limit;
    private String replyToSuffix;

    public byte[] toBytes() {
        return CacheUtils.toBytes(this);
    }
}
