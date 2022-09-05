package ru.geosteering.goperform.cache.model.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import ru.geosteering.commonModels.wits.RecordIndex;

@Getter
@Setter
public class Comment {
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Double key;
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private RecordIndex recordIndex;
    private String title;
    private String text;
}
