package ru.geosteering.goperform.cache.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.repository.CurveCacheDTO;
import ru.geosteering.goperform.cache.repository.CurveCacheRepository;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static ru.geosteering.goperform.cache.config.Config.BATCH_SIZE;
import static ru.geosteering.goperform.cache.config.Config.MARGIN_SIZE;

@Component
@Slf4j
public class Storage {

    private final CurveCacheRepository repository;

    private final Map<Long, PriorityQueue<CacheItem>> realTimeCache;
    private final Map<Long, PriorityQueue<CacheItem>> historyCache;
    private final Set<Long> historyLoaded;
    private final Set<Long> activeCurves;
    private final Set<CurveCacheDTO> errorBuffer;
    //TODO error buffer -> what to do?

    public Storage(CurveCacheRepository repository) {
        this.repository = repository;
        realTimeCache = new ConcurrentHashMap<>();
        historyCache = new ConcurrentHashMap<>();
        historyLoaded = ConcurrentHashMap.newKeySet();
        activeCurves = ConcurrentHashMap.newKeySet();
        errorBuffer = ConcurrentHashMap.newKeySet();
    }

    public void add(Long id, CacheItem item, boolean isReal) {

        Map<Long, PriorityQueue<CacheItem>> cache = isReal ? realTimeCache : historyCache;

        PriorityQueue<CacheItem> items =
                cache.computeIfAbsent(id, val -> new PriorityQueue<>(BATCH_SIZE + MARGIN_SIZE, Comparator.comparing(CacheItem::getKey)));

        synchronized (items) {
            items.add(item);
            transferIfNeed(id, items, isReal);
        }

        if (isReal) {
            activeCurves.add(id);
        }
    }

    public void addAll(Long id, Collection<CacheItem> collection, boolean isReal) {

        if (collection != null && !collection.isEmpty()) {
            Map<Long, PriorityQueue<CacheItem>> cache = isReal ? realTimeCache : historyCache;

            PriorityQueue<CacheItem> items =
                    cache.computeIfAbsent(id, val -> new PriorityQueue<>(collection.size(), Comparator.comparing(CacheItem::getKey)));

            synchronized (items) {
                items.addAll(collection);
                transferIfNeed(id, items, isReal);
            }
        }
    }

    private void transferIfNeed(Long id, PriorityQueue<CacheItem> items, boolean isReal) {
        if ((isReal && !isHistoryLoaded(id)) || items.size() < BATCH_SIZE + MARGIN_SIZE) {
            return;
        }

        List<CurveCacheDTO> transferList = new ArrayList<>((items.size() - MARGIN_SIZE) / BATCH_SIZE);

        fillTransferList(id, items, transferList);

        if (!transferList.isEmpty()) {
            try {
                repository.save(transferList);
            } catch (Exception e) {
                errorBuffer.addAll(transferList);
                log.error("Database exception: {}. Data added to errorBuffer", e.getMessage(), e);
            }
        }
    }

    private void fillTransferList(Long id, PriorityQueue<CacheItem> items, List<CurveCacheDTO> transferList) {

        while (items.size() >= BATCH_SIZE + MARGIN_SIZE) {
            ArrayList<CacheItem> itemsBatch = new ArrayList<>(BATCH_SIZE);
            for (int i = 0; i < BATCH_SIZE; i++) {
                itemsBatch.add(items.poll());
            }
            transferList.add(new CurveCacheDTO(id, itemsBatch));
        }
    }

    public void mergeCache(Long id) {

        PriorityQueue<CacheItem> realItems = realTimeCache.get(id);

        synchronized (realItems) {

            PriorityQueue<CacheItem> historyItems = historyCache.get(id);

            if (historyItems != null && !historyItems.isEmpty()) {
                int before = historyItems.size();
                log.info("Start merging caches. Before merging: id={}, real_size={}, hist_size={}", id, realItems.size(), before);

                historyItems.removeAll(realItems);
                log.info("Found {} duplicates", before - historyItems.size());

                realItems.addAll(historyItems);
                historyItems.clear();

                transferIfNeed(id, realItems, true);
            }
        }

        historyCache.remove(id);
    }

    public synchronized String getInfo() {

        int realTotal = realTimeCache.values().stream().mapToInt(PriorityQueue::size).sum();
        int historyTotal = historyCache.values().stream().mapToInt(PriorityQueue::size).sum();

        return String.format("Realtime cache: curves - %d; records - %d. History cache: curves - %d; records - %d. History loaded: %s. Error buffer size: %d",
                realTimeCache.size(), realTotal, historyCache.size(), historyTotal, historyLoaded, errorBuffer.size());
    }

    public Set<Long> getActiveCurves() {
        synchronized (activeCurves) {
            return Set.copyOf(activeCurves);
        }
    }

    public CacheItem getFirstReal(Long id) {

        if (realTimeCache.containsKey(id)) {
            PriorityQueue<CacheItem> items = realTimeCache.get(id);

            synchronized (items) {
                return realTimeCache.get(id).peek();
            }
        }
        return null;
    }

    public CacheItem getLastHistory(Long id) {

        CacheItem lastHistory = null;

        if (historyCache.containsKey(id)) {
            PriorityQueue<CacheItem> items = historyCache.get(id);

            synchronized (items) {
                lastHistory = items.stream()
                        .max(Comparator.comparing(CacheItem::getKey))
                        .orElse(null);
            }
        }

        if (lastHistory == null) {
            lastHistory = repository.getEmptyLast(id);
        }

        return lastHistory;
    }

    public boolean isActiveCurve(Long id) {
        return activeCurves.contains(id);
    }

    public void setHistoryLoaded(Long id) {
        historyLoaded.add(id);
    }

    public boolean isHistoryLoaded(Long id) {
        return historyLoaded.contains(id);
    }

    public void onRestartHistory() {
        historyCache.clear();
        historyLoaded.clear();
    }

    public void onRestartReal() {
        realTimeCache.clear();
        activeCurves.clear();
    }

    public List<CacheItem> getFromStorage(Long id, Double from, Double to) {
        List<CacheItem> result = getFromStorage(id, from, to, false);
        result.addAll(getFromStorage(id, from, to, true));
        return result;
    }

    private List<CacheItem> getFromStorage(Long id, Double from, Double to, boolean isReal) {
        ArrayList<CacheItem> result = new ArrayList<>();

        Map<Long, PriorityQueue<CacheItem>> cache = isReal ? realTimeCache : historyCache;

        if (cache.containsKey(id)) {
            PriorityQueue<CacheItem> items = cache.get(id);
            synchronized (items) {
                if (!items.isEmpty() && items.peek().getKey() <= to) {
                    items.stream()
                            .filter(item -> item.getKey() >= from && item.getKey() <= to)
                            .forEach(result::add);
                }
            }
        }

        if (!result.isEmpty()) {
            result.sort(Comparator.comparing(CacheItem::getKey));
        }

        return result;
    }
}
