package ru.geosteering.goperform.cache.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.SegmentDto;
import ru.geosteering.goperform.cache.utils.MapperUtils;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class Approximator {

    private final Config config;
    private final MainRepository repository;
    private final MetaDataProcessor metaDataProcessor;
    private final Map<Long, CurveApproximator> approximators = new ConcurrentHashMap<>();

    @PostConstruct
    private void loadLostItems() {

        List<Long> ids = repository.getAllItemIds();
        log.info("Try to load lost items. Loaded {} ids from db", ids.size());

        for (Long id : ids) {
            loadLostById(id);
        }
    }

    public void loadLostById(Long id) {

        if (isDateTimeCurve(id)) {

            CurveApproximator approximator = approximators.computeIfAbsent(id, k -> new CurveApproximator(id));

            Map<Integer, Double> scaleLast = new HashMap<>();

            for (Integer scale : config.SCALE_MINUTES) {

                if (isApproximatedScale(findItemsOnPixel(id, scale))) {
                    Double last = repository.getLastSegment(id, scale);
                    last = last == null ? 0 : last;
                    scaleLast.put(scale, last);
                }
            }

            Double from = scaleLast.values().stream().min(Double::compareTo).orElse(0.0);

            List<CurveItem> items = repository.getItemsFromTo(id, from, Double.MAX_VALUE)
                    .stream()
                    .flatMap((Function<String, Stream<CurveItem>>) str -> MapperUtils.parseListOf(str, CurveItem.class).stream())
                    .collect(Collectors.toList());

            scaleLast.forEach((scale, last) -> {
                List<CurveItem> lost = items.stream()
                        .filter(i -> i.getKey() >= last)
                        .collect(Collectors.toList());

                approximator.collectItems(lost, scale, findItemsOnPixel(id, scale));
            });

            log.debug("{} lost items for id {} loaded", items.size(), id);
        }
    }

    private boolean isDateTimeCurve(Long id) {
        return metaDataProcessor.getMetaData(id).getIndexType() == LogIndexType.DATE_TIME;
    }

    private int findItemsOnPixel(Long id, int scale) {

        MetaData metaData = metaDataProcessor.getMetaData(id);

        int totalSeconds = (int) ((metaData.getLastDBKey() - metaData.getFirstDBKey()) / 1000);
        int totalItems = metaData.getItemsInDB();
        int secondsOnPixel = scale * 60 / 120;

        return (int) ((long) secondsOnPixel * totalItems / totalSeconds);
    }

    private boolean isApproximatedScale(int itemsOnPixel) {
        return itemsOnPixel >= 5;
    }

    public void collectItemsBatch(Long id, List<CurveItem> items) {

        LogIndexType indexType = metaDataProcessor.getIndexType(id);
        LogDataType dataType = metaDataProcessor.getDataType(id);

        if (indexType == LogIndexType.DATE_TIME && (dataType == LogDataType.DOUBLE || dataType == LogDataType.LONG)) {

            CurveApproximator curveApproximator = approximators.computeIfAbsent(id, k -> new CurveApproximator(id));

            synchronized (curveApproximator) {
                curveApproximator.collectItemsBatch(items);
            }
        }
    }

    public List<CurveSegment> getAndCompleteSegmentsFromMemory(Long id, int scale, Double from, Double to, List<CurveItem> items) {

        if (approximators.containsKey(id)) {

            return approximators.get(id)
                    .getAndCompleteSegments(scale, from, to, items);
        }

        return Collections.emptyList();
    }

    public void resetById(Long id) {
        approximators.remove(id);
        loadLostById(id);
    }

    private class CurveApproximator {

        private final Long curveId;
        private final Map<Integer, List<CurveSegment>> collector; // scale / segments
        private final Map<Integer, CurveItem> lastItems;

        public CurveApproximator(Long curveId) {
            this.curveId = curveId;
            this.collector = new HashMap<>();
            this.lastItems = new HashMap<>();

            for (int i : config.SCALE_MINUTES) {
                collector.put(i, new ArrayList<>());
            }
        }

        private void collectItemsBatch(List<CurveItem> items) {

            if (isDateTimeCurve(curveId)) {

                for (Map.Entry<Integer, List<CurveSegment>> entry : collector.entrySet()) {

                    Integer scale = entry.getKey();
                    int itemsOnPixel = findItemsOnPixel(curveId, scale);

                    if (isApproximatedScale(itemsOnPixel)) {

                        collectItems(items, scale, itemsOnPixel);

                    } else {
                        log.debug("Curve id {} items {} NOT ADDED for approximating in scale {} with density {} points/pxl", curveId, items.size(), scale, itemsOnPixel);
                    }
                }
            }
        }

        private void collectItems(List<CurveItem> items, int scale, int itemsOnPixel) {

            List<CurveSegment> segments = collector.get(scale);

            CurveSegment lastSegment = segments.isEmpty() ? null : segments.get(segments.size() - 1);

            int secondsOnPixel = scale * 60 / 120;
            int linesBatch = Math.min(1000, 10_000 / itemsOnPixel);

            for (CurveItem item : items) {

                if (lastSegment != null && item.getKey() - lastSegment.getFirstKey() < (secondsOnPixel - 1) * 1000) {

                    lastSegment.addItem(item);

                } else {

                    if (segments.size() >= linesBatch) {
                        repository.saveSegments(SegmentDto.fromLinesList(curveId, scale, segments));
                        log.debug("{} lines saved: curve id {}, scale {}, seconds/pxl {}, points/pxl {}", segments.size(), curveId, scale, secondsOnPixel, itemsOnPixel);
                        segments.clear();
                    }

                    CurveSegment segment = new CurveSegment();

                    CurveItem lastItem = lastItems.get(scale);
                    if (lastItem != null && item.getKey() - lastItem.getKey() < (secondsOnPixel - 1) * 1000) {
                        segment.addItem(lastItem);
                    }

                    segment.addItem(item);

                    segments.add(segment);

                    lastSegment = segment;
                }

                lastItems.put(scale, item);
            }

            metaDataProcessor.getMetaData(curveId).getScaleSet().add(scale);
        }

        private List<CurveSegment> getAndCompleteSegments(int scale, Double from, Double to, List<CurveItem> items) {

            List<CurveSegment> result = new ArrayList<>();

            if (collector.containsKey(scale)) {

                List<CurveSegment> segments = collector.get(scale);

                synchronized (segments) {

                    if (from == null && to == null) {

                        result.addAll(segments);
                        addItemsToSegmentList(result, items, scale);

                    } else {

                        double finalFrom = from == null ? Double.MIN_VALUE : from;
                        double finalTo = to == null ? Double.MAX_VALUE : to;

                        if (!segments.isEmpty() && segments.get(0).getFirstKey() <= finalTo) {

                            segments.stream()
                                    .filter(seg -> seg.getFirstKey() <= finalTo && seg.getLastKey() >= finalFrom)
                                    .forEach(result::add);
                        }

                        if (!items.isEmpty() && items.get(0).getKey() <= finalTo) {

                            List<CurveItem> collect = items.stream()
                                    .filter(item -> item.getKey() <= finalTo && item.getKey() >= finalFrom)
                                    .collect(Collectors.toList());

                            addItemsToSegmentList(result, collect, scale);
                        }
                    }
                }
            }
            return result;
        }

        private void addItemsToSegmentList(List<CurveSegment> segments, List<CurveItem> items, int scale) {

            CurveSegment lastSegment = segments.isEmpty() ? null : segments.get(segments.size() - 1);

            int secondsOnPixel = scale * 60 / 120;

            CurveItem lastItem = lastItems.get(scale);

            for (CurveItem item : items) {

                if (lastSegment != null && item.getKey() - lastSegment.getFirstKey() < (secondsOnPixel - 1) * 1000) {

                    lastSegment.addItem(item);

                } else {

                    CurveSegment segment = new CurveSegment();

                    if (lastItem != null && item.getKey() - lastItem.getKey() < (secondsOnPixel - 1) * 1000) {
                        segment.addItem(lastItem);
                    }

                    segment.addItem(item);

                    segments.add(segment);

                    lastSegment = segment;
                }

                lastItem = item;
            }
        }
    }
}
