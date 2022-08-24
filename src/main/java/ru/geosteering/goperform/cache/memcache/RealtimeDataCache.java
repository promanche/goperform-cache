package ru.geosteering.goperform.cache.memcache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.processor.EventBus;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.item.ItemsBatch;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataCache {

    private final MainRepository repository;
    private final Config config;
    private final MetaDataCache metaDataCache;
    private final EventBus eventBus;

    private final Map<Long, PriorityQueue<CurveItem>> realTimeCache = new ConcurrentHashMap<>();

    public void add(Long id, CurveItem item) {

        PriorityQueue<CurveItem> items =
                realTimeCache.computeIfAbsent(id, key -> new PriorityQueue<>(config.BATCH_SIZE + config.MARGIN_SIZE,
                        Comparator.comparing(CurveItem::getKey)));

        synchronized (items) {
            items.add(item);
            save(id, items);
        }
    }

    public void remove(Long id) {
        realTimeCache.remove(id);
    }

    public void removeAll() {
        realTimeCache.clear();
    }

    public CurveItem getFirst(Long id) {

        CurveItem firstRealItem = null;

        if (realTimeCache.containsKey(id)) {
            PriorityQueue<CurveItem> items = realTimeCache.get(id);

            synchronized (items) {
                firstRealItem = realTimeCache.get(id).peek();
            }
        }

        return firstRealItem;
    }

    private void save(Long id, PriorityQueue<CurveItem> items) {

        if (metaDataCache.isHistoryLoaded(id)) {

            while (items.size() >= config.BATCH_SIZE + config.MARGIN_SIZE) {

                ArrayList<CurveItem> itemsBatch = new ArrayList<>(config.BATCH_SIZE);
                for (int i = 0; i < config.BATCH_SIZE; i++) {
                    itemsBatch.add(items.poll());
                }

                repository.saveItems(List.of(ItemDto.fromItemsList(id, itemsBatch)));

                eventBus.post(new ItemsBatch(id, itemsBatch));
            }
        }
    }

    public void merge(Long curveId, List<CurveItem> histItems) {

        PriorityQueue<CurveItem> items = realTimeCache.get(curveId);

        synchronized (items) {

            int before = items.size();
            log.info("Curve {}. Before merging caches: real_size={}, hist_size={}", curveId, items.size(), before);

            items.removeAll(histItems);

            log.info("Curve {}. Found {} duplicates. After merging caches: real_size={}", curveId, before - items.size(), items.size());

            items.addAll(histItems);

            save(curveId, items);
        }
    }

    public List<CurveItem> get(Long id, Double from, Double to) {

        ArrayList<CurveItem> result = new ArrayList<>();

        if (realTimeCache.containsKey(id)) {

            PriorityQueue<CurveItem> items = realTimeCache.get(id);

            synchronized (items) {

                if (from == null && to == null) {
                    result.addAll(items);

                } else {
                    double finalFrom = from == null ? Double.MIN_VALUE : from;
                    double finalTo = to == null ? Double.MAX_VALUE : to;

                    if (!items.isEmpty() && Double.compare(items.peek().getKey(), finalTo) <= 0) {
                        items.stream()
                                .filter(item -> Double.compare(item.getKey(), finalFrom) >= 0 && Double.compare(item.getKey(), finalTo) <= 0)
                                .forEach(result::add);
                    }
                }
            }
        }

        if (!result.isEmpty()) {
            result.sort(Comparator.comparing(CurveItem::getKey));
        }

        return result;
    }
}
