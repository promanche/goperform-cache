package ru.geosteering.goperform.cache.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.commonModels.dataService.CurveDataItem;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;

import java.util.*;

@Slf4j
public class CacheUtils {

    private final static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.findAndRegisterModules();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public static ApiMessage parseApiMessage(String json, String subject) {
        try {
            return mapper.readValue(json, ApiMessage.class);
        } catch (JsonProcessingException e) {
            log.warn("Parsing ApiMessage exception. Message: {}, subject: {}", json, subject);
            return null;
        }
    }

    public static List<CurveDataItem> parseCurveDataItems(String json) {
        try {
            return new ArrayList<>(Arrays.asList(mapper.readValue(json, CurveDataItem[].class)));
        } catch (JsonProcessingException e) {
            log.error("Parsing CurveDataItem[] exception: {}", json);
            return new ArrayList<>();
        }
    }

    public static byte[] toBytes(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            log.error("Object to bytes exception: {}", obj);
            return new byte[0];
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
        try {
            String[] arr = subject.split("\\.");
            return Long.parseLong(arr[arr.length - 1]);
        } catch (Exception e) {
            log.error("Parsing id from subject exception: {}", subject);
            return null;
        }
    }
}
