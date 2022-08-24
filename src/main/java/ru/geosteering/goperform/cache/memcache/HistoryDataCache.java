package ru.geosteering.goperform.cache.memcache;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.item.ItemsBatch;
import ru.geosteering.goperform.cache.processor.EventBus;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class HistoryDataCache {

    private final MainRepository repository;
    private final Config config;
    private final EventBus eventBus;

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

            eventBus.post(new ItemsBatch(id, itemsBatch));
        }

        repository.saveItems(transfer);
    }

    public void removeAll() {
        historyCache.clear();
    }

    public CurveItem getLast(Long id) {

        if (historyCache.containsKey(id)) {

            PriorityQueue<CurveItem> items = historyCache.get(id);

            synchronized (items) {
                return items.stream()
                        .max(Comparator.comparing(CurveItem::getKey))
                        .orElse(null);
            }
        }

        return null;
    }

    public List<CurveItem> drain(Long id) {

        PriorityQueue<CurveItem> items = historyCache.remove(id);

        List<CurveItem> result = new ArrayList<>(items.size());

        while (!items.isEmpty()) {
            result.add(items.poll());
        }

        return result;
    }

    public void remove(Long id) {
        historyCache.remove(id);
    }

    public List<CurveItem> get(Long id, Double from, Double to) {

        ArrayList<CurveItem> result = new ArrayList<>();

        if (historyCache.containsKey(id)) {

            PriorityQueue<CurveItem> items = historyCache.get(id);

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
