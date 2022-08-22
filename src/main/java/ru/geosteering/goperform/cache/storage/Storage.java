package ru.geosteering.goperform.cache.storage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;
import ru.geosteering.goperform.cache.service.Approximator;
import ru.geosteering.goperform.cache.service.MetaDataProcessor;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class Storage {

    private final MainRepository repository;
    private final Config config;
    private final MetaDataProcessor metaDataProcessor;
    private final Approximator approximator;

    private final Map<Long, PriorityQueue<CurveItem>> realTimeCache = new ConcurrentHashMap<>();
    private final Map<Long, PriorityQueue<CurveItem>> historyCache = new ConcurrentHashMap<>();
    private final Set<Long> historyLoaded = ConcurrentHashMap.newKeySet();
    private final Map<Long, LocalDateTime> activeCurves = new ConcurrentHashMap<>();

    @Scheduled(fixedRate = 1, timeUnit = TimeUnit.MINUTES)
    private void checkActivity() {

        synchronized (activeCurves) {
            synchronized (realTimeCache) {

                LocalDateTime now = LocalDateTime.now();

                activeCurves.entrySet().removeIf(entry -> {

                    boolean isNotActive = entry.getValue().isBefore(now.minusMinutes(1));

                    if (isNotActive) {
                        log.info("Curve {} is not active. Last item time: {}", entry.getKey(), entry.getValue());
                        realTimeCache.remove(entry.getKey());
                        historyLoaded.remove(entry.getKey());
                    }

                    return isNotActive;
                });
            }
        }
    }

    public void add(Long id, CurveItem item, boolean isReal) {

        Map<Long, PriorityQueue<CurveItem>> cache = isReal ? realTimeCache : historyCache;

        PriorityQueue<CurveItem> items =
                cache.computeIfAbsent(id, key -> new PriorityQueue<>(config.BATCH_SIZE + config.MARGIN_SIZE,
                        Comparator.comparing(CurveItem::getKey)));

        synchronized (items) {
            items.add(item);
            save(id, items, isReal);
        }

        if (isReal) {
            if (!activeCurves.containsKey(id)) {
                historyLoaded.remove(id);
                log.info("New active curve {}", id);
            }
            activeCurves.put(id, LocalDateTime.now());
        }
    }

    public void addAll(Long id, Collection<CurveItem> collection, boolean isReal) {

        if (collection != null && !collection.isEmpty()) {
            Map<Long, PriorityQueue<CurveItem>> cache = isReal ? realTimeCache : historyCache;

            PriorityQueue<CurveItem> items =
                    cache.computeIfAbsent(id, val -> new PriorityQueue<>(collection.size(), Comparator.comparing(CurveItem::getKey)));

            synchronized (items) {
                items.addAll(collection);
                save(id, items, isReal);
            }
        }
    }

    private void save(Long id, PriorityQueue<CurveItem> items, boolean isReal) {
        if ((isReal && !isHistoryLoaded(id))) {
            return;
        }

        List<ItemDto> itemDtoList = getDtoList(id, items, isReal);

        if (!itemDtoList.isEmpty()) {
            repository.saveItems(itemDtoList);
        }
    }

    private List<ItemDto> getDtoList(Long id, PriorityQueue<CurveItem> items, boolean isReal) {

        List<ItemDto> itemDtoList = new ArrayList<>(items.size() / config.BATCH_SIZE);

        int limit = isReal ? config.BATCH_SIZE + config.MARGIN_SIZE : config.BATCH_SIZE;

        while (items.size() >= limit) {

            ArrayList<CurveItem> itemsBatch = new ArrayList<>(config.BATCH_SIZE);
            for (int i = 0; i < config.BATCH_SIZE; i++) {
                itemsBatch.add(items.poll());
            }

            itemDtoList.add(ItemDto.fromItemsList(id, itemsBatch));

            metaDataProcessor
                    .updateByItemsBatch(id, itemsBatch.get(0).getKey(), itemsBatch.get(itemsBatch.size() - 1).getKey(), itemsBatch.size());

            approximator.collectItemsBatch(id, itemsBatch);
        }

        return itemDtoList;
    }

    public void mergeCache(Long id) {

        PriorityQueue<CurveItem> realItems = realTimeCache.get(id);

        synchronized (realItems) {

            PriorityQueue<CurveItem> historyItems = historyCache.get(id);

            if (historyItems != null && !historyItems.isEmpty()) {
                int before = historyItems.size();
                log.info("Start merging caches. Before merging: id={}, real_size={}, hist_size={}", id, realItems.size(), before);

                historyItems.removeAll(realItems);
                log.info("Found {} duplicates", before - historyItems.size());

                realItems.addAll(historyItems);
                historyItems.clear();

                save(id, realItems, true);
            }
        }

        historyCache.remove(id);
    }

    public synchronized String getInfo() {

        int realTotal = realTimeCache.values().stream().mapToInt(PriorityQueue::size).sum();
        int historyTotal = historyCache.values().stream().mapToInt(PriorityQueue::size).sum();

        return String.format("Realtime cache: curves - %d; records - %d. History cache: curves - %d; records - %d. " +
                        "History loaded: count - %d, curves - %s",
                realTimeCache.size(), realTotal, historyCache.size(), historyTotal, historyLoaded.size(), historyLoaded);
    }

    public Set<Long> getActiveCurves() {
        synchronized (activeCurves) {
            return Set.copyOf(activeCurves.keySet());
        }
    }

    public String getFirstRealKey(Long id) {

        if (realTimeCache.containsKey(id)) {
            PriorityQueue<CurveItem> items = realTimeCache.get(id);

            synchronized (items) {
                CurveItem item = realTimeCache.get(id).peek();
                if (item != null) {
                    return getKeyAsString(item.getKey(), metaDataProcessor.getIndexType(id));
                }
            }
        }
        return null;
    }

    public String getLastHistoryKey(Long id) {

        CurveItem lastHistoryItem = null;

        if (historyCache.containsKey(id)) {
            PriorityQueue<CurveItem> items = historyCache.get(id);

            synchronized (items) {
                lastHistoryItem = items.stream()
                        .max(Comparator.comparing(CurveItem::getKey))
                        .orElse(null);
            }
        }

        if (lastHistoryItem != null) {
            return getKeyAsString(lastHistoryItem.getKey(), metaDataProcessor.getIndexType(id));
        } else {
            MetaData metaData = metaDataProcessor.getMetaData(id);
            return getKeyAsString(metaData.getLastDBKey(), metaData.getIndexType());
        }
    }

    private String getKeyAsString(Double key, LogIndexType type) {

        if (key == null) {
            return null;
        }

        return type == LogIndexType.MEASURED_DEPTH ?
                new BigDecimal(key).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() :
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
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

    public synchronized void clearHistoryData() {
        historyCache.clear();
        historyLoaded.clear();
    }

    public synchronized void clearRealTimeData() {
        realTimeCache.clear();
        activeCurves.clear();
    }

    public List<CurveItem> getFromStorage(Long id, Double from, Double to) {
        List<CurveItem> result = getFromStorage(id, from, to, false);
        result.addAll(getFromStorage(id, from, to, true));
        return result;
    }

    private List<CurveItem> getFromStorage(Long id, Double from, Double to, boolean isReal) {
        ArrayList<CurveItem> result = new ArrayList<>();

        Map<Long, PriorityQueue<CurveItem>> cache = isReal ? realTimeCache : historyCache;

        if (cache.containsKey(id)) {

            PriorityQueue<CurveItem> items = cache.get(id);

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
            result.sort(Comparator.comparing(CurveItem::getKey));
        }

        return result;
    }

    public void resetById(Long id) {
        historyCache.remove(id);
        realTimeCache.remove(id);
        historyLoaded.remove(id);
        activeCurves.remove(id);
    }
}
