package ru.geosteering.goperformcache.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.goperformcache.model.CurveDataMessage;

import java.io.IOException;

@Slf4j
public class CacheUtils {

    private final static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.findAndRegisterModules();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public static CurveDataMessage parseCurveDataMessage(String json, String subject) {
        CurveDataMessage curveDataMessage;
        try {
            curveDataMessage = mapper.readValue(json, CurveDataMessage.class);
        } catch (IOException e) {
            log.warn("Parsing CurveDataMessage exception. Message: {}, subject: {}", json, subject);
            return null;
        }
        return curveDataMessage;
    }

    public static byte[] toBytes(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            log.error("Object to bytes exception: {}", obj);
            return new byte[0];
        }
    }

    public static String getFieldFromJson(String json, String fieldName) {
        try {
            return mapper.readTree(json).get(fieldName).asText();
        } catch (JsonProcessingException e) {
            log.error("Field {} not found in {}", fieldName, json);
            return "";
        }
    }

    public static String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("Object to json exception: {}", obj);
            return "";
        }
    }

    public static Long getIdFromSubject(String subject) {
        Long id = null;
        try {
            String[] arr = subject.split("\\.");
            id = Long.parseLong(arr[arr.length - 1]);
        } catch (Exception e) {
            log.error("Parsing id from subject exception: {}", e.getMessage(), e);
        }
        return id;
    }
}
