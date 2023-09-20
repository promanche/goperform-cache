package ru.geosteering.goperform.cache.model.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import ru.geosteering.commonModels.wits.RecordIndex;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Getter
@Setter
@ToString
public class Comment {

    @NotNull
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Double key;

    @NotNull
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private RecordIndex recordIndex;

    @NotBlank
    private String title;

    @NotBlank
    private String text;

    private double length;
}
