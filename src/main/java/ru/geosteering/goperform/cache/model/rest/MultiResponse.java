package ru.geosteering.goperform.cache.model.rest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.CurveSegment;

import java.util.*;

@Getter
@RequiredArgsConstructor
public class MultiResponse {

    private final long[] ids;
    @JsonIgnore
    private final Map<Long, Data<?>> dataMap = new HashMap<>();
    @JsonProperty("data")
    private final Set<Data<?>> dataSet = new TreeSet<>();

    public void addSegments(long id, List<CurveSegment> segments) {
        int index = findIndex(id);

        if (index >= 0) {
            for (CurveSegment segment : segments) {
                SegmentsData data;
                long key = segment.getFirstKey().longValue();
                if (dataMap.containsKey(key)) {
                    data = (SegmentsData) dataMap.get(key);
                } else {
                    data = new SegmentsData(new long[2], new Object[ids.length]);
                    data.getT()[0] = key;
                    data.getT()[1] = segment.getLastKey().longValue();
                    dataMap.put(key, data);
                    dataSet.add(data);
                }
                data.v[index] = new double[]{segment.getMinVal(), segment.getMaxVal()};
            }
        }
    }

    public void addItems(long id, List<CurveItem> items) {
        int index = findIndex(id);

        if (index >= 0) {
            for (CurveItem item : items) {
                ItemData data;
                long key = item.getKey().longValue();
                if (dataMap.containsKey(key)) {
                    data = (ItemData) dataMap.get(key);
                } else {
                    data = new ItemData(key, new Object[ids.length]);
                    dataMap.put(key, data);
                    dataSet.add(data);
                }
                data.v[index] = item.getValue();
            }
        }
    }

    private int findIndex(long id) {
        for (int i = 0; i < ids.length; i++) {
            if (Objects.equals(ids[i], id)) {
                return i;
            }
        }
        return -1;
    }

    @RequiredArgsConstructor
    @Getter
    private static abstract class Data<T> implements Comparable<Data<T>> {
        private final T t;
    }

    @Getter
    private static class ItemData extends Data<Long> {
        private final Object[] v;

        public ItemData(Long t, Object[] v) {
            super(t);
            this.v = v;
        }

        @Override
        public int compareTo(Data<Long> o) {
            return Long.compare(this.getT(), o.getT());
        }
    }

    @Getter
    private static class SegmentsData extends Data<long[]> {
        private final Object[] v;

        public SegmentsData(long[] t, Object[] v) {
            super(t);
            this.v = v;
        }

        @Override
        public int compareTo(Data<long[]> o) {
            return Long.compare(this.getT()[0], o.getT()[0]);
        }
    }
}
