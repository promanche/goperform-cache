package ru.geosteering.goperform.cache.model.rest;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateCurveRequest {

    private String curveName;
    private TypeCurve typeCurve;

    public enum TypeCurve {
        COMMENTS,
        FREEZE
    }
}
