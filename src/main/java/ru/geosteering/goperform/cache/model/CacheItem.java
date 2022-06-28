package ru.geosteering.goperform.cache.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.commonModels.dataService.AbstractDataItem;

import java.io.IOException;
import java.math.BigDecimal;

@Setter
@Getter
@ToString
@EqualsAndHashCode
@Slf4j
public class CacheItem {
    @JsonIgnore
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    ItemType type;
    @JsonSerialize(using = CustomDoubleSerializer.class)
    Double key;
    Object value;

    public static CacheItem fromCurveDataItem(AbstractDataItem<?> dataItem) {
        CacheItem cacheItem = new CacheItem();
        cacheItem.type = dataItem.getTime() == null ? ItemType.DEPTH : ItemType.TIME;
        cacheItem.key = dataItem.getTime() == null ? dataItem.getDepth() : dataItem.getTime().toInstant().toEpochMilli();
        cacheItem.value = dataItem.getValue();
        return cacheItem;
    }

    public static class CustomDoubleSerializer extends JsonSerializer<Double> {

        @Override
        public void serialize(Double value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeNumber(new BigDecimal(value).toPlainString());
        }
    }
}
