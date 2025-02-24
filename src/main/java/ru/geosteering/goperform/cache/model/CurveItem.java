package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.geosteering.goperform.cache.utils.CustomDoubleSerializer;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CurveItem {

    @JsonSerialize(using = CustomDoubleSerializer.class)
    private Double key;
    private Object value;
}
