package ru.geosteering.goperform.cache.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.CacheItem;

import java.util.*;

import static java.lang.Math.*;

@Slf4j
public class CacheUtils {

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

    public static List<CacheItem> parseCacheItems(String json) {
        try {
            return new ArrayList<>(Arrays.asList(mapper.readValue(json, CacheItem[].class)));
        } catch (JsonProcessingException e) {
            log.error("Parsing CurveDataItem[] exception: {}", json, e);
            return new ArrayList<>();
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

    public static List<CacheItem> approximate(List<CacheItem> points, double factor) {

        if (factor >= 1 || factor <= 0) {
            throw new IllegalArgumentException("Factor must be in the open range (0, 1)");
        }

        if (points == null || points.size() < 3) {
            throw new IllegalArgumentException("List of points must not be null and must contains more than 2 points");
        }

        boolean[] keepPoint = new boolean[points.size()];

        double minSign = (double) findMaxDist(points, 0, points.size() - 1)[1] * (1 - factor);

        List<Integer[]> segments = new LinkedList<>();
        segments.add(new Integer[]{0, points.size() - 1});

        while (!segments.isEmpty()) {
            Integer[] indexes = segments.remove(0);
            int from = indexes[0];
            int to = indexes[1];

            keepPoint[from] = true;
            keepPoint[to] = true;

            Number[] maxDistPoint = findMaxDist(points, from, to);
            int maxIndex = (int) maxDistPoint[0];
            double maxDist = (double) maxDistPoint[1];

            if (maxDist > minSign) {
                keepPoint[maxIndex] = true;
                segments.add(new Integer[]{from, maxIndex});
                segments.add(new Integer[]{maxIndex, to});
            }
        }


        List<CacheItem> result = new ArrayList<>();
        for (int i = 0; i < points.size(); i++) {
            if (keepPoint[i]) {
                result.add(points.get(i));
            }
        }

        return result;
    }

    private static Number[] findMaxDist(List<CacheItem> points, int from, int to) {

        int maxIndex = -1;
        double maxDistance = 0;

        for (int i = from + 1; i < to; i++) {
            CacheItem current = points.get(i);
            double distance = findDistance(points.get(from), points.get(to), current);
            if (distance > maxDistance) {
                maxIndex = i;
                maxDistance = distance;
            }
        }

        return new Number[]{maxIndex, maxDistance};
    }

    private static double findDistance(CacheItem first, CacheItem last, CacheItem current) {
        double x0 = current.getKey();
        double y0 = (double) current.getValue();

        double x1 = first.getKey();
        double y1 = (double) first.getValue();

        double x2 = last.getKey();
        double y2 = (double) last.getValue();

        double A = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1);
        double B = sqrt(pow((x2 - x1), 2) + pow((y2 - y1), 2));

        return B == 0 ? 0 : A / B;
    }
}
