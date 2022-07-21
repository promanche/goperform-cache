package ru.geosteering.goperform.cache.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.model.ItemType;
import ru.geosteering.goperform.cache.repository.CacheDTO;
import ru.geosteering.goperform.cache.repository.CacheRepository;

import javax.annotation.PostConstruct;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class Storage {

    private final CacheRepository repository;
    private final Config config;

    private final Map<Long, PriorityQueue<CacheItem>> realTimeCache;
    private final Map<Long, PriorityQueue<CacheItem>> historyCache;
    private final Set<Long> historyLoaded;
    private final Map<Long, LocalDateTime> activeCurves;
    private final Map<Long, CacheItem> lastDBItems;

    public Storage(CacheRepository repository, Config config) {
        this.repository = repository;
        this.config = config;
        realTimeCache = new ConcurrentHashMap<>();
        historyCache = new ConcurrentHashMap<>();
        historyLoaded = ConcurrentHashMap.newKeySet();
        activeCurves = new ConcurrentHashMap<>();
        lastDBItems = new ConcurrentHashMap<>();
    }

    @PostConstruct
    public void loadLastKeys() {
        List<CacheDTO> allLast = repository.getAllLast();
        allLast.forEach(dto -> {
            CacheItem item = new CacheItem();
            item.setKey(dto.getLast());
            item.setType(dto.getType());

            lastDBItems.put(dto.getCurveId(), item);
        });
        log.debug("Last items map loaded: {}", lastDBItems);
    }

    public void add(Long id, CacheItem item, boolean isReal) {

        Map<Long, PriorityQueue<CacheItem>> cache = isReal ? realTimeCache : historyCache;

        PriorityQueue<CacheItem> items =
                cache.computeIfAbsent(id, val -> new PriorityQueue<>(config.BATCH_SIZE + config.MARGIN_SIZE, Comparator.comparing(CacheItem::getKey)));

        synchronized (items) {
            items.add(item);
            transferIfNeed(id, items, isReal);
        }

        if (isReal) {
            if (!activeCurves.containsKey(id)) {
                historyLoaded.remove(id);
                log.info("New active curve id {}", id);
            }
            activeCurves.put(id, LocalDateTime.now());
        }
    }

    @Scheduled(fixedRate = 20, timeUnit = TimeUnit.SECONDS)
    private void checkActivity() {
        synchronized (activeCurves) {
            log.debug("checkActivity synchronized on activeCurves");
            synchronized (realTimeCache) {
                log.debug("checkActivity synchronized on realTimeCache");
                LocalDateTime now = LocalDateTime.now();
                activeCurves.entrySet().removeIf(entry -> {
                    boolean isNotActive = entry.getValue().isBefore(now.minusSeconds(20));
                    if (isNotActive) {
                        log.warn("Curve id {} is not active", entry.getKey());
                        realTimeCache.remove(entry.getKey());
                    }
                    return isNotActive;
                });
            }
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
        if ((isReal && !isHistoryLoaded(id)) || items.size() < config.BATCH_SIZE + config.MARGIN_SIZE) {
            return;
        }

        List<CacheDTO> transferList = new ArrayList<>((items.size() - config.MARGIN_SIZE) / config.BATCH_SIZE);

        fillTransferList(id, items, transferList);

        if (!transferList.isEmpty()) {
            try {
                repository.save(transferList);
            } catch (Exception e) {
                log.error("Database exception: {}. Data added to errorBuffer", e.getMessage(), e);
            }
        }
    }

    private void fillTransferList(Long id, PriorityQueue<CacheItem> items, List<CacheDTO> transferList) {

        while (items.size() >= config.BATCH_SIZE + config.MARGIN_SIZE) {
            ArrayList<CacheItem> itemsBatch = new ArrayList<>(config.BATCH_SIZE);
            for (int i = 0; i < config.BATCH_SIZE; i++) {
                itemsBatch.add(items.poll());
            }
            transferList.add(new CacheDTO(id, itemsBatch));
            rememberLastDbItem(id, itemsBatch.get(itemsBatch.size() - 1));
        }
    }

    private void rememberLastDbItem(Long id, CacheItem item) {
        lastDBItems.put(id, item);
    }

    public Double getLastDbKey(Long id) {
        if (lastDBItems.containsKey(id)) {
            return lastDBItems.get(id).getKey();
        }
        return null;
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

        return String.format("Realtime cache: curves - %d; records - %d. History cache: curves - %d; records - %d. History loaded: %s",
                realTimeCache.size(), realTotal, historyCache.size(), historyTotal, historyLoaded);
    }

    public Set<Long> getActiveCurves() {
        synchronized (activeCurves) {
            return Set.copyOf(activeCurves.keySet());
        }
    }

    public String getFirstRealKey(Long id) {

        if (realTimeCache.containsKey(id)) {
            PriorityQueue<CacheItem> items = realTimeCache.get(id);

            synchronized (items) {
                CacheItem item = realTimeCache.get(id).peek();
                if (item != null) {
                    return getKeyAsString(item.getKey(), item.getType());
                }
            }
        }
        return null;
    }

    public String getLastHistoryKey(Long id) {

        CacheItem lastHistoryItem;

        if (historyCache.containsKey(id)) {
            PriorityQueue<CacheItem> items = historyCache.get(id);

            synchronized (items) {
                lastHistoryItem = items.stream()
                        .max(Comparator.comparing(CacheItem::getKey))
                        .orElse(null);
            }
        } else {
            lastHistoryItem = lastDBItems.get(id);
        }

        if (lastHistoryItem != null) {
            return getKeyAsString(lastHistoryItem.getKey(), lastHistoryItem.getType());
        }

        return null;
    }

    private String getKeyAsString(Double key, ItemType type) {

        if (key == null || type == null) {
            return null;
        }

        if (type == ItemType.TIME) {
            return OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
        } else {
            return String.valueOf(key);
        }
    }

    public boolean isActiveCurve(Long id) {
        return activeCurves.containsKey(id);
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

                if (from == null && to == null) {
                    result.addAll(items);

                } else {
                    double finalFrom = from == null ? Double.MIN_VALUE : from;
                    double finalTo = to == null ? Double.MAX_VALUE : to;

                    if (!items.isEmpty() && items.peek().getKey() <= finalTo) {
                        items.stream()
                                .filter(item -> item.getKey() >= finalFrom && item.getKey() <= finalTo)
                                .forEach(result::add);
                    }
                }
            }
        }

        if (!result.isEmpty()) {
            result.sort(Comparator.comparing(CacheItem::getKey));
        }

        return result;
    }

    public synchronized void resetById(Long id) {
        historyCache.remove(id);
        realTimeCache.remove(id);
        historyLoaded.remove(id);
        activeCurves.remove(id);

        CacheDTO lastDto = repository.getLast(id);
        CacheItem item = new CacheItem();
        item.setType(lastDto.getType());
        item.setKey(lastDto.getLast());

        lastDBItems.put(id, item);

    }
}
