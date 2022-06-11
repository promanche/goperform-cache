package ru.geosteering.goperformcache.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperformcache.model.CurveDataItem;
import ru.geosteering.goperformcache.repository.CurveCacheRepository;
import ru.geosteering.goperformcache.repository.dto.CurveCacheDTO;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static ru.geosteering.goperformcache.config.Config.BATCH_SIZE;
import static ru.geosteering.goperformcache.config.Config.MARGIN_SIZE;

@Component
@Slf4j
public class Storage {

    private final CurveCacheRepository repository;

    private final Map<Long, PriorityQueue<CurveDataItem>> realTimeCache;
    private final Map<Long, PriorityQueue<CurveDataItem>> historyCache;
    private final Set<Long> historyLoaded;
    private final Set<CurveCacheDTO> errorBuffer;
    //TODO error buffer -> what to do?

    public Storage(CurveCacheRepository repository) {
        this.repository = repository;
        realTimeCache = new ConcurrentHashMap<>();
        historyCache = new ConcurrentHashMap<>();
        historyLoaded = ConcurrentHashMap.newKeySet();
        errorBuffer = ConcurrentHashMap.newKeySet();
    }

    public void add(Long id, CurveDataItem item, boolean isReal) {

        Map<Long, PriorityQueue<CurveDataItem>> cache = isReal ? realTimeCache : historyCache;

        PriorityQueue<CurveDataItem> items =
                cache.computeIfAbsent(id, val -> new PriorityQueue<>(BATCH_SIZE + MARGIN_SIZE, Comparator.comparing(CurveDataItem::getTime)));

        synchronized (items) {
            items.add(item);
            transferIfNeed(id, items, isReal);
        }
    }

    public void addAll(Long id, Collection<CurveDataItem> collection, boolean isReal) {

        Map<Long, PriorityQueue<CurveDataItem>> cache = isReal ? realTimeCache : historyCache;

        PriorityQueue<CurveDataItem> items =
                cache.computeIfAbsent(id, val -> new PriorityQueue<>(BATCH_SIZE + MARGIN_SIZE, Comparator.comparing(CurveDataItem::getTime)));

        synchronized (items) {
            items.addAll(collection);
            transferIfNeed(id, items, false);
        }
    }

    private void transferIfNeed(Long id, PriorityQueue<CurveDataItem> items, boolean isReal) {
        if (isReal && !isHistoryLoaded(id)) {
            return;
        }

        List<CurveCacheDTO> transferList = new ArrayList<>();
        fillTransferList(id, items, transferList);

        if (!transferList.isEmpty()) {
            try {
                repository.save(transferList);
            } catch (Exception e) {
                errorBuffer.addAll(transferList);
                log.error("Database exception: {}. Data added to errorBuffer", e.getMessage());
            }
        }
    }

    private void fillTransferList(Long id, PriorityQueue<CurveDataItem> items, List<CurveCacheDTO> transferList) {

        if (items.size() >= BATCH_SIZE + MARGIN_SIZE) {
            ArrayList<CurveDataItem> itemsBatch = new ArrayList<>(BATCH_SIZE);
            for (int i = 0; i < BATCH_SIZE; i++) {
                itemsBatch.add(items.poll());
            }
            transferList.add(new CurveCacheDTO(id, itemsBatch));
            fillTransferList(id, items, transferList);
        }
    }

    public void mergeCache(Long id) {

        PriorityQueue<CurveDataItem> realItems = realTimeCache.get(id);

        synchronized (realItems) {

            PriorityQueue<CurveDataItem> historyItems = historyCache.get(id);


            int before = historyItems.size();
            log.info("Start merging caches. Before merging: id={}, real_size={}, hist_size={}", id, realItems.size(), before);
            historyItems.removeAll(realItems);
            log.info("Found {} duplicates", before - historyItems.size());

            realItems.addAll(historyItems);
            historyItems.clear();

            transferIfNeed(id, realItems, true);

            log.info("End merging caches. After merging: id={}, real_size={}, hist_size={}", id, realItems.size(), historyItems.size());
        }
    }

    public synchronized String getInfo() {

        int realTotal = realTimeCache.values().stream().mapToInt(PriorityQueue::size).sum();
        int historyTotal = historyCache.values().stream().mapToInt(PriorityQueue::size).sum();

        return String.format("Realtime cache: curves - %d; records - %d. History cache: curves - %d; records - %d. History loaded: %s. Error buffer size: %d",
                realTimeCache.size(), realTotal, historyCache.size(), historyTotal, historyLoaded, errorBuffer.size());
    }

    public Set<Long> getActiveCurves() {
        return Set.copyOf(realTimeCache.keySet());
    }

    public OffsetDateTime getFirstReal(Long id) {

        OffsetDateTime first;
        PriorityQueue<CurveDataItem> items = realTimeCache.get(id);

        synchronized (items) {
            CurveDataItem item = realTimeCache.get(id).peek();
            first = item == null ? null : item.getTime();
        }

        return first;
    }

    public OffsetDateTime getFirstHistory(Long id) {

        OffsetDateTime first = repository.getMinFirst(id);

        if (first == null) {

            PriorityQueue<CurveDataItem> items = historyCache.get(id);

            synchronized (items) {
                CurveDataItem item = items.peek();
                first = item == null ? null : item.getTime();
            }
        }

        return first;
    }

    public OffsetDateTime getLastHistory(Long id) {

        CurveDataItem curveDataItem;
        OffsetDateTime result;
        PriorityQueue<CurveDataItem> items = historyCache.get(id);

        synchronized (items) {
            curveDataItem = items.stream()
                    .max(Comparator.comparing(CurveDataItem::getTime))
                    .orElse(null);

            result = curveDataItem == null ? repository.getMaxLast(id) : curveDataItem.getTime();
        }

        return result;
    }

    public boolean isActiveCurve(Long id) {
        return realTimeCache.containsKey(id);
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
    }
}
