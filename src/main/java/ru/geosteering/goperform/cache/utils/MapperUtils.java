package ru.geosteering.goperform.cache.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.*;

import java.util.*;

@Slf4j
public class MapperUtils {

    private final static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.findAndRegisterModules();
    }

    public static ApiMessage parseApiMessage(String json, String subject) {
        try {
            return mapper.readValue(json, ApiMessage.class);
        } catch (JsonProcessingException e) {
            log.warn("Parsing ApiMessage exception. Message: {}, subject: {}", json, subject);
            return null;
        }
    }

    public static List<CurveItem> parseCacheItems(String json) {
        try {
            return new ArrayList<>(Arrays.asList(mapper.readValue(json, CurveItem[].class)));
        } catch (JsonProcessingException e) {
            log.error("Parsing CurveDataItem[] exception: {}", json, e);
            return Collections.emptyList();
        }
    }

    public static MetaData parseMetaData(String json) {
        try {
            return mapper.readValue(json, MetaData.class);
        } catch (JsonProcessingException e) {
            log.error("Parsing MetaData exception from string: {}", json, e);
            return null;
        }
    }

    public static List<CurveSegment> parseCacheLines(String json) {
        try {
            return new ArrayList<>(Arrays.asList(mapper.readValue(json, CurveSegment[].class)));
        } catch (JsonProcessingException e) {
            log.error("Parsing CurveDataItem[] exception: {}", json, e);
            return Collections.emptyList();
        }
    }

    public static byte[] toBytes(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            log.error("Object to bytes exception: {}", obj, e);
            return new byte[0];
        }
    }

    public static String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("Object to json exception: {}", obj, e);
            return "";
        }
    }

    public static Long getIdFromSubject(String subject) {
        try {
            String[] arr = subject.split("\\.");
            return Long.parseLong(arr[arr.length - 1]);
        } catch (Exception e) {
            log.error("Parsing id from subject exception: {}", subject, e);
            return null;
        }
    }
}
