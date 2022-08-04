package ru.geosteering.goperform.cache.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.SegmentDto;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class Approximator {

    private final Config config;
    private final MainRepository repository;
    private final MetaDataProcessor metaDataProcessor;
    private final Map<Long, CurveApproximator> approximators = new ConcurrentHashMap<>();

    @PostConstruct
    private void loadItems() {
        //TODO
    }

    public void addItemsBatch(Long id, List<CurveItem> items) {
        LogIndexType indexType = metaDataProcessor.getIndexType(id);
        LogDataType dataType = metaDataProcessor.getDataType(id);
        if (indexType == LogIndexType.DATE_TIME && (dataType == LogDataType.DOUBLE || dataType == LogDataType.LONG)) {
            CurveApproximator curveApproximator = approximators.computeIfAbsent(id, k -> new CurveApproximator(id));
            synchronized (curveApproximator) {
                curveApproximator.addItemsBatch(items);
            }
        }
    }

    public List<CurveSegment> getLines(Long id, int scale, Double from, Double to) {
        if (approximators.containsKey(id)) {
            return approximators.get(id)
                    .getSegments(scale, from, to);
        }

        return Collections.emptyList();
    }

    public void onRestart() {
        approximators.clear();
        loadItems();
    }

    public void resetById(Long id) {
        approximators.remove(id);
    }

    private class CurveApproximator {

        private final Long curveId;
        private final Map<Integer, List<CurveSegment>> collector;

        public CurveApproximator(Long curveId) {
            this.curveId = curveId;
            this.collector = new HashMap<>();

            for (int i : config.SCALE_MINUTES) {
                collector.put(i, new ArrayList<>());
            }
        }

        public void addItemsBatch(List<CurveItem> items) {

            MetaData metaData = metaDataProcessor.getMetaData(curveId);

            if (metaData.getIndexType() == LogIndexType.DATE_TIME) {

                int totalSeconds = (int) ((metaData.getLastDBKey() - metaData.getFirstDBKey()) / 1000);
                int totalItems = metaData.getItemsInDB();

                for (Map.Entry<Integer, List<CurveSegment>> entry : collector.entrySet()) {

                    Integer scale = entry.getKey();
                    int secondsOnPixel = scale * 60 / 120;
                    int itemsOnPixel = secondsOnPixel * totalItems / totalSeconds;

                    if (itemsOnPixel > 5) {

                        log.debug("Curve id {} items {} added for approximating in scale {} with density {} points/pxl", curveId, items.size(), scale, itemsOnPixel);

                        List<CurveSegment> segments = entry.getValue();

                        CurveSegment last = segments.isEmpty() ? null : segments.get(segments.size() - 1);

                        for (CurveItem item : items) {

                            if (last != null && item.getKey() - last.getFirstKey() < (secondsOnPixel - 1) * 1000) {
                                Double value = (Double) item.getValue();
                                if (value > last.getMaxVal()) {
                                    last.setMaxVal(value);
                                }
                                if (value < last.getMinVal()) {
                                    last.setMinVal(value);
                                }
                                last.setLastKey(item.getKey());

                            } else {

                                if (segments.size() == config.BATCH_SIZE) {
                                    repository.saveLines(SegmentDto.fromLinesList(curveId, scale, segments));
                                    segments.clear();
                                }

                                CurveSegment segment = new CurveSegment();
                                segment.setFirstKey(item.getKey());
                                segment.setLastKey(item.getKey());
                                segment.setMinVal((Double) item.getValue());
                                segment.setMaxVal((Double) item.getValue());
                                segments.add(segment);

                                last = segment;
                            }
                        }

                        metaData.getScaleSet().add(scale);

                    } else {
                        log.debug("Curve id {} items {} NOT ADDED for approximating in scale {} with density {} points/pxl", curveId, items.size(), scale, itemsOnPixel);
                    }
                }
            }
        }

        public List<CurveSegment> getSegments(int scale, Double from, Double to) {

            List<CurveSegment> result = new ArrayList<>();

            if (collector.containsKey(scale)) {

                List<CurveSegment> segments = collector.get(scale);

                synchronized (segments) {

                    if (from == null && to == null) {
                        result.addAll(segments);
                    } else {

                        double finalFrom = from == null ? Double.MIN_VALUE : from;
                        double finalTo = to == null ? Double.MAX_VALUE : to;

                        if (!segments.isEmpty() && segments.get(0).getLastKey() <= finalTo) {
                            segments.stream()
                                    .filter(seg -> seg.getFirstKey() <= finalTo && seg.getLastKey() >= finalFrom)
                                    .forEach(result::add);
                        }
                    }
                }
            }

            return result;
        }
    }
}
