package ru.geosteering.goperform.cache.model.rest;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.CurveSegment;

import java.util.*;

@Getter
@RequiredArgsConstructor
public class MultiResponse {

    private final long[] ids;
    private ArrayList<MultiData> data;

    public void addSegments(long id, List<CurveSegment> segments) {
        int idIndex = findIdIndex(id);

        if (idIndex >= 0) {
            if (data == null) {
                data = new ArrayList<>(segments.size() + 1000);
            }

            for (CurveSegment segment : segments) {
                long key = segment.getFirstKey().longValue();
                MultiData multiData;
                if (data.isEmpty() || ((long[]) data.get(data.size() - 1).t)[0] < key) {
                    multiData = new MultiData(new long[]{key, segment.getLastKey().longValue()}, new Object[ids.length]);
                    multiData.v[idIndex] = new double[]{segment.getMinVal(), segment.getMaxVal()};
                    data.add(multiData);
                } else if (key < ((long[]) data.get(0).t)[0]) {
                    multiData = new MultiData(new long[]{key, segment.getLastKey().longValue()}, new Object[ids.length]);
                    multiData.v[idIndex] = new double[]{segment.getMinVal(), segment.getMaxVal()};
                    data.add(0, multiData);
                } else {
                    int dataIndex = findDataIndex(key, false);
                    multiData = data.get(dataIndex);
                    if (((long[]) multiData.t)[0] == key) {
                        multiData.v[idIndex] = new double[]{segment.getMinVal(), segment.getMaxVal()};
                    } else {
                        multiData = new MultiData(new long[]{key, segment.getLastKey().longValue()}, new Object[ids.length]);
                        multiData.v[idIndex] = new double[]{segment.getMinVal(), segment.getMaxVal()};
                        data.add(dataIndex + 1, multiData);
                    }
                }
            }
        }
    }

    public void addItems(long id, List<CurveItem> items) {
        int idIndex = findIdIndex(id);

        if (idIndex >= 0) {
            if (data == null) {
                data = new ArrayList<>(items.size() + 1000);
            }

            for (CurveItem item : items) {
                long key = item.getKey().longValue();
                MultiData multiData;
                if (data.isEmpty() || (long) data.get(data.size() - 1).t < key) {
                    multiData = new MultiData(key, new Object[ids.length]);
                    multiData.v[idIndex] = item.getValue();
                    data.add(multiData);
                } else if (key < (long) data.get(0).t) {
                    multiData = new MultiData(key, new Object[ids.length]);
                    multiData.v[idIndex] = item.getValue();
                    data.add(0, multiData);
                } else {
                    int dataIndex = findDataIndex(key, true);
                    multiData = data.get(dataIndex);
                    if ((long) multiData.t == key) {
                        multiData.v[idIndex] = item.getValue();
                    } else {
                        multiData = new MultiData(key, new Object[ids.length]);
                        multiData.v[idIndex] = item.getValue();
                        data.add(dataIndex + 1, multiData);
                    }
                }
            }
        }
    }

    private int findIdIndex(long id) {
        for (int i = 0; i < ids.length; i++) {
            if (Objects.equals(ids[i], id)) {
                return i;
            }
        }
        return -1;
    }

    private int findDataIndex(long t, boolean isItem) {
        int leftIndex = 0;
        int rightIndex = data.size() - 1;

        while (true) {
            int index = leftIndex + (rightIndex - leftIndex) / 2;
            if (leftIndex + 1 >= rightIndex) {
                return index;
            }
            long key = isItem ? (long) data.get(index).t : ((long[]) data.get(index).t)[0];
            if (key == t) {
                return index;
            } else if (key > t) {
                rightIndex = index;
            } else {
                leftIndex = index;
            }
        }
    }

    @RequiredArgsConstructor
    @Getter
    private static class MultiData {
        final Object t;
        final Object[] v;
    }
}
