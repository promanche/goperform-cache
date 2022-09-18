package ru.geosteering.goperform.cache.model.rest;

import lombok.*;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Getter
@Setter
@ToString
public class CreateCurveRequest {

    @NotNull
    private Long logId;

    @NotBlank
    private String curveName;

    @NotNull
    private TypeCurve typeCurve;

    public enum TypeCurve {
        COMMENTS,
        FREEZE
    }
}
