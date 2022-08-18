package ru.geosteering.goperform.cache.memcache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.ItemsBatchCollected;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;
import ru.geosteering.goperform.cache.service.EventBus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealTimeCurveDataCache {

    private final MainRepository repository;
    private final Config config;
    private final MetaDataCache metaDataCache;

    private final Map<Long, PriorityQueue<CurveItem>> realTimeCache = new ConcurrentHashMap<>();

    public void add(Long id, CurveItem item) {

        PriorityQueue<CurveItem> items =
                realTimeCache.computeIfAbsent(id, key -> new PriorityQueue<>(config.BATCH_SIZE + config.MARGIN_SIZE, Comparator.comparing(CurveItem::getKey)));

        synchronized (items) {
            items.add(item);
            save(id, items);
        }
    }

    public void clearById(Long id) {
        realTimeCache.remove(id);
    }

    public void clearAll() {
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

                EventBus.post(new ItemsBatchCollected(id, itemsBatch));
            }
        }
    }
}
