package ru.geosteering.goperform.cache.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;

@Slf4j
public class StaticMapper {

    private final static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.findAndRegisterModules();
        mapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static <T> T parseObject(String json, Class<T> clazz) {

        if (json == null || json.isEmpty()) {
            return null;
        }

        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error("Parsing object exception: json {}, class {}", json, clazz.getSimpleName(), e);
            return null;
        }
    }

    public static <T> List<T> parseListOf(String json, Class<T> clazz) {

        if (json == null || json.isEmpty()) {
            return null;
        }

        try {
            return mapper.readValue(json, mapper.getTypeFactory().constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            log.error("Parsing list of {} exception: json {}", clazz.getSimpleName(), json, e);
            return Collections.emptyList();
        }
    }

    public static byte[] toBytes(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            log.error("Object to bytes exception: {}", obj, e);
            return new byte[0];
        }
    }

    public static String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Object to json exception: {}", obj, e);
            return "";
        }
    }
}
