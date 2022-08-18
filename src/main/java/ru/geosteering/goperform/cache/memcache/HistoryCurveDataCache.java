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
public class HistoryCurveDataCache {

    private final MainRepository repository;
    private final Config config;

    private final Map<Long, PriorityQueue<CurveItem>> historyCache = new ConcurrentHashMap<>();

    public void add(Long id, Collection<CurveItem> items) {

        PriorityQueue<CurveItem> queue =
                historyCache.computeIfAbsent(id, key -> new PriorityQueue<>(config.HISTORY_REQUEST_LIMIT, Comparator.comparing(CurveItem::getKey)));

        synchronized (queue) {
            queue.addAll(items);
            save(id, queue);
        }
    }

    private void save(Long id, PriorityQueue<CurveItem> items) {

        List<ItemDto> transfer = new ArrayList<>();

        while (items.size() >= config.BATCH_SIZE) {

            ArrayList<CurveItem> itemsBatch = new ArrayList<>(config.BATCH_SIZE);
            for (int i = 0; i < config.BATCH_SIZE; i++) {
                itemsBatch.add(items.poll());
            }

            transfer.add(ItemDto.fromItemsList(id, itemsBatch));

            EventBus.post(new ItemsBatchCollected(id, itemsBatch));
        }

        repository.saveItems(transfer);
    }

    public void clearAll() {
        historyCache.clear();
    }
}
