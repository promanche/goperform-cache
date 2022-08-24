package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.model.event.Event;
import ru.geosteering.goperform.cache.model.event.item.*;
import ru.geosteering.goperform.cache.model.event.task.ClearTask;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.SegmentDto;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
@Slf4j
public class CurveSegmentProcessor implements EventProcessor {

    private final Config config;
    private final MetaDataCache metaDataCache;
    private final EventBus eventBus;
    private final MainRepository repository;

    private final Map<Long, SingleCurveSegmenter> segmenters = new ConcurrentHashMap<>();

    @PostConstruct
    private void register() {
        eventBus.register(
                List.of(
                        Event.EventType.ITEMS_BATCH,
                        Event.EventType.CLEAR_TASK
                ),
                this);

        loadLostItems();
    }

    @Override
    public void processEvent(Event event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case ITEMS_BATCH:
                collectItemsBatch((ItemsBatch) event);
                break;
            case CLEAR_TASK:
                remove((ClearTask) event);
                break;
            default:
                break;
        }
    }

    private void loadLostItems() {

        List<Long> ids = repository.getAllItemIds();
        log.info("Start loading lost items. {} ids in db found", ids.size());

        for (Long id : ids) {
            loadLostById(id);
        }

        log.info("Finish loading lost items");

    }

    public void loadLostById(Long id) {
        log.info("1");
        if (isApproximatedCurve(id)) {

            SingleCurveSegmenter segmenter = segmenters.computeIfAbsent(id, k -> new SingleCurveSegmenter(id));

            Map<Integer, Double> scaleLast = new HashMap<>();

            for (Integer scale : config.SCALE_MINUTES) {

                if (isApproximatedScale(findItemsOnPixel(id, scale))) {
                    Double last = repository.getLastSegment(id, scale);
                    last = last == null ? Double.MIN_VALUE : last;
                    scaleLast.put(scale, last);
                }
            }
            log.info("2");
            Double from = scaleLast.values().stream().min(Double::compareTo).orElse(Double.MIN_VALUE);

            List<CurveItem> items = repository.getItemsFromTo(id, from, Double.MAX_VALUE)
                    .stream()
                    .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                    .collect(Collectors.toList());

            scaleLast.forEach((scale, last) -> {
                List<CurveItem> lost = items.stream()
                        .filter(i -> Double.compare(i.getKey(), last) >= 0)
                        .collect(Collectors.toList());

                segmenter.lastItems.put(scale, lost.get(0));
                segmenter.collectItems(lost, scale, findItemsOnPixel(id, scale));
            });
            log.info("3");
            log.debug("{} lost items for id {} loaded", items.size(), id);
        }
    }

    private void collectItemsBatch(ItemsBatch event) {

        Long id = event.getId();
        List<CurveItem> items = event.getItems();

        if (isApproximatedCurve(id)) {

            SingleCurveSegmenter segmenter = segmenters.computeIfAbsent(id, k -> new SingleCurveSegmenter(id));

            synchronized (segmenter) {
                segmenter.collectItemsBatch(items);
            }
        }
    }

    private boolean isApproximatedCurve(Long id) {

        MetaData metaData = metaDataCache.getMetaData(id);

        return metaData.getIndexType() != LogIndexType.MEASURED_DEPTH && metaData.getAxisDefinition() == null
                && (metaData.getTypeLogData() == LogDataType.DOUBLE || metaData.getTypeLogData() == LogDataType.LONG);
    }

    private int findItemsOnPixel(Long id, int scale) {

        MetaData metaData = metaDataCache.getMetaData(id);

        int totalSeconds = (int) ((metaData.getLastDBKey() - metaData.getFirstDBKey()) / 1000);
        int totalItems = metaData.getItemsInDB();
        int secondsOnPixel = scale * 60 / 120;

        return (int) ((long) secondsOnPixel * totalItems / totalSeconds);
    }

    private boolean isApproximatedScale(int itemsOnPixel) {
        return itemsOnPixel >= 5;
    }

    public List<CurveSegment> getAndCompleteSegmentsFromMemory(Long id, int scale, Double from, Double to, List<CurveItem> items) {

        if (segmenters.containsKey(id)) {

            return segmenters.get(id)
                    .getAndCompleteSegments(scale, from, to, items);
        }

        return Collections.emptyList();
    }

    private void remove(ClearTask event) {
        Long id = event.getId();

        segmenters.remove(id);

        loadLostById(id);
    }

    private class SingleCurveSegmenter {

        private final Long id;
        private final Map<Integer, List<CurveSegment>> collector = new HashMap<>(); // scale -> segments
        private final Map<Integer, CurveItem> lastItems = new HashMap<>(); // scale -> lastItem

        public SingleCurveSegmenter(Long id) {
            this.id = id;

            for (int i : config.SCALE_MINUTES) {
                collector.put(i, new ArrayList<>());
            }
        }

        private void collectItemsBatch(List<CurveItem> items) {

            for (Map.Entry<Integer, List<CurveSegment>> entry : collector.entrySet()) {

                Integer scale = entry.getKey();
                int itemsOnPixel = findItemsOnPixel(id, scale);

                if (isApproximatedScale(itemsOnPixel)) {

                    collectItems(items, scale, itemsOnPixel);

                } else {
                    log.debug("Curve id {} items {} NOT ADDED for approximating in scale {} with density {} points/pxl", id, items.size(), scale, itemsOnPixel);
                }
            }
        }

        private void collectItems(List<CurveItem> items, int scale, int itemsOnPixel) {

            List<CurveSegment> segments = collector.get(scale);

            CurveSegment lastSegment = segments.isEmpty() ? null : segments.get(segments.size() - 1);

            int secondsOnPixel = scale * 60 / 120;

            for (CurveItem item : items) {

                if (lastSegment != null && item.getKey() - lastSegment.getFirstKey() < (secondsOnPixel - 1) * 1000) {

                    lastSegment.addItem(item);

                } else {

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

            if (segments.size() > 1) {

                segments.remove(segments.size() - 1);

                repository.saveSegments(SegmentDto.fromLinesList(id, scale, segments));
                log.debug("{} lines saved: curve id {}, scale {}, seconds/pxl {}, points/pxl {}", segments.size(), id, scale, secondsOnPixel, itemsOnPixel);
                segments.clear();

                segments.add(lastSegment);
            }

            metaDataCache.getMetaData(id).getScaleSet().add(scale);
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

                        if (!segments.isEmpty() && Double.compare(segments.get(0).getFirstKey(), finalTo) <= 0) {

                            segments.stream()
                                    .filter(seg -> Double.compare(seg.getFirstKey(), finalTo) <= 0 && Double.compare(seg.getLastKey(), finalFrom) >= 0)
                                    .forEach(result::add);
                        }

                        if (!items.isEmpty() && Double.compare(items.get(0).getKey(), finalTo) <= 0) {

                            List<CurveItem> collect = items.stream()
                                    .filter(item -> Double.compare(item.getKey(), finalTo) <= 0 && Double.compare(item.getKey(), finalFrom) >= 0)
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
