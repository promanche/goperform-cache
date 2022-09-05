package ru.geosteering.goperform.cache.memcache;

import lombok.RequiredArgsConstructor;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.repository.MainRepository;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public abstract class AbstractCurveItemCache {

    protected final MainRepository repository;
    protected final Config config;

    protected final Map<Long, PriorityQueue<CurveItem>> cache = new ConcurrentHashMap<>();

    public void add(Long id, CurveItem item) {

        PriorityQueue<CurveItem> queue =
                cache.computeIfAbsent(id, k -> new PriorityQueue<>(config.HISTORY_REQUEST_LIMIT, Comparator.comparing(CurveItem::getKey)));

        synchronized (queue) {
            queue.add(item);
            save(id, queue);
        }
    }

    public void addAll(Long id, Collection<CurveItem> items) {

        PriorityQueue<CurveItem> queue =
                cache.computeIfAbsent(id, k -> new PriorityQueue<>(config.HISTORY_REQUEST_LIMIT, Comparator.comparing(CurveItem::getKey)));

        synchronized (queue) {
            queue.addAll(items);
            save(id, queue);
        }
    }

    abstract void save(Long id, PriorityQueue<CurveItem> items);

    public void remove(Long id) {
        cache.remove(id);
    }

    public void removeAll() {
        cache.clear();
    }

    public List<CurveItem> get(Long id, Double from, Double to) {

        ArrayList<CurveItem> result = new ArrayList<>();

        if (cache.containsKey(id)) {

            PriorityQueue<CurveItem> items = cache.get(id);

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

    public CurveItem getFirst(Long id) {

        CurveItem firstRealItem = null;

        if (cache.containsKey(id)) {
            PriorityQueue<CurveItem> items = cache.get(id);

            synchronized (items) {
                firstRealItem = cache.get(id).peek();
            }
        }

        return firstRealItem;
    }

    public CurveItem getLast(Long id) {

        if (cache.containsKey(id)) {

            PriorityQueue<CurveItem> items = cache.get(id);

            synchronized (items) {
                return items.stream()
                        .max(Comparator.comparing(CurveItem::getKey))
                        .orElse(null);
            }
        }

        return null;
    }
}
