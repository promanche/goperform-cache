package ru.geosteering.goperform.cache.processor;

import io.nats.client.Message;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.*;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class CurveDataLoadProcessor implements DefaultEventProcessor {

    private final Config config;
    private final MetaDataCache metaDataCache;
    private final RealtimeDataCache realtimeDataCache;
    private final HistoryDataCache historyDataCache;
    private final MainRepository repository;

    private final Map<Long, LoadStatus> loadMap = new ConcurrentHashMap<>();
    private final Map<Long, ReloadData> reloadMap = new ConcurrentHashMap<>();
    private final Set<Long> waitingBlock = ConcurrentHashMap.newKeySet();
    private final AtomicInteger requestAllowed = new AtomicInteger();
    private final Map<Long, Long> timer = new ConcurrentHashMap<>(); // id -> request start nanos

    private ScheduledExecutorService scheduler;

    @Override
    public void onConnect() {

        loadMap.entrySet()
                .forEach(entry -> {
                    if (entry.getValue() != LoadStatus.BLOCKED || !reloadMap.containsKey(entry.getKey())) {
                        entry.setValue(LoadStatus.IN_QUEUE);
                    }
                });

        scheduler = Executors.newSingleThreadScheduledExecutor();

        requestAllowed.set(config.HISTORY_ONETIME_REQUESTS);

        scheduler.scheduleAtFixedRate(this::load, 30000, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onDisconnect() {
        timer.clear();
        scheduler.shutdownNow();
    }

    @Override
    public void onRealtimeCurveItem(Long id, CurveItem item) {
        if (!metaDataCache.isHistoryLoaded(id)) {
            loadMap.putIfAbsent(id, LoadStatus.IN_QUEUE);
        }
    }

    @Override
    public void onOldItem(Long id, CurveItem item) {

        block(id);

        LocalDateTime reloadTime = LocalDateTime.now().plusMinutes(5);
        Double from = item.getKey();

        addReloadData(id, reloadTime, from);
    }

    @Override
    public void onLoadResult(Long id, LoadResult result, Double from, Double to) {

        log.info("Load request processing time {} sec", (System.nanoTime() - timer.remove(id)) / 1_000_000_000.0);

        synchronized (loadMap) {

            if (waitingBlock.contains(id)) {

                waitingBlock.remove(id);
                log.info("{} blocked", id);
                loadMap.put(id, LoadStatus.BLOCKED);

            } else if (result == LoadResult.DONE) {

                if (metaDataCache.isActive(id)) {
                    PriorityQueue<CurveItem> items = historyDataCache.drain(id);
                    realtimeDataCache.merge(id, items);
                }

                loadMap.remove(id);

            } else {

                loadMap.put(id, LoadStatus.IN_QUEUE);
            }

        }

        requestAllowed.incrementAndGet();
    }

    private void load() {

        try {
            if (requestAllowed.getAndDecrement() > 0) {

                synchronized (loadMap) {

                    Map.Entry<Long, LoadStatus> entry = loadMap.entrySet().stream()
                            .filter(ent -> ent.getValue() == LoadStatus.IN_QUEUE)
                            .findFirst().orElse(null);

                    if (entry != null) {

                        entry.setValue(LoadStatus.IN_PROGRESS);
                        loadCurveData(entry.getKey());

                    } else {

                        requestAllowed.incrementAndGet();
                    }
                }

            } else {

                requestAllowed.incrementAndGet();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void loadCurveData(Long id) {

        String from = findFrom(id);
        String to = findTo(id);

        CurveDataRequest request = new CurveDataRequest(id, from, to, null, false, false, config.HISTORY_REQUEST_LIMIT, config.HISTORY_NUID + "." + id);

        timer.put(id, System.nanoTime());
        Message message = NatsConnector.sendRequest(config.SUBJECT, StaticMapper.toBytes(request));

        if (message == null) {
            log.warn("Nats response is null (curve {})", id);
            loadMap.computeIfPresent(id, (k, v) -> LoadStatus.IN_QUEUE);
        }
    }

    private String findFrom(Long id) {

        CurveItem lastHistory = historyDataCache.getLast(id);

        MetaData metaData = metaDataCache.getMetaData(id);

        Double key = lastHistory == null ? metaData.getLastDBKey() : lastHistory.getKey();

        if (key == null) {
            return null;
        }

        return getKeyAsString(key, metaData.getIndexType());
    }

    private String findTo(Long id) {

        CurveItem firstReal = realtimeDataCache.getFirst(id);
        Double key = firstReal == null ? null : firstReal.getKey();

        if (key == null) {
            return null;
        }

        return getKeyAsString(firstReal.getKey(), metaDataCache.getMetaData(id).getIndexType());
    }

    private String getKeyAsString(Double key, LogIndexType type) {

        if (key == null) {
            return null;
        }

        return type == LogIndexType.MEASURED_DEPTH ?
                new BigDecimal(key).setScale(4, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString() :
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
    }

    private void block(Long id) {

        synchronized (loadMap) {

            if (loadMap.get(id) == LoadStatus.IN_PROGRESS) {

                waitingBlock.add(id);

            } else {

                log.info("{} blocked", id);
                loadMap.put(id, LoadStatus.BLOCKED);
            }
        }
    }


    private void addReloadData(Long id, LocalDateTime reloadTime, Double from) {

        synchronized (reloadMap) {

            ReloadData reloadData = reloadMap.get(id);

            if (reloadData != null) {

                reloadData.setReloadTime(reloadTime);

                if (Double.compare(reloadData.getFrom(), from) > 0) {
                    reloadData.setFrom(from);
                }

            } else {

                ReloadData newData = new ReloadData();
                newData.setReloadTime(reloadTime);
                newData.setFrom(from);

                reloadMap.put(id, newData);
            }

        }
    }

    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.SECONDS)
    public void reload() {

        if (!reloadMap.isEmpty()) {

            LocalDateTime now = LocalDateTime.now();

            Set.copyOf(reloadMap.keySet()).forEach(id -> {

                ReloadData reloadData = reloadMap.get(id);

                if (reloadData.getReloadTime().isBefore(now) && loadMap.get(id) == LoadStatus.BLOCKED) {

                    log.info("{} start reload process from {}", id, reloadData.getFrom());

                    metaDataCache.removeFromLoaded(id);

                    repository.deleteItems(id, reloadData.getFrom());
                    repository.deleteSegments(id, reloadData.getFrom());

                    EventDispatcher.getInstance().onReloadData(id, reloadData.getFrom());

                    reloadMap.remove(id);

                    loadMap.put(id, LoadStatus.IN_QUEUE);
                }
            });
        }
    }

    public void reloadByRequest(Long id, Double from) {

        try {

            block(id);

            while (loadMap.get(id) != LoadStatus.BLOCKED) {
                log.info("{} block waiting", id);
                Thread.sleep(100);
            }

            addReloadData(id, LocalDateTime.now(), from);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void loadByRequest(Long id) {
        loadMap.putIfAbsent(id, LoadStatus.IN_QUEUE);
    }

    @Scheduled(fixedDelay = 60, timeUnit = TimeUnit.SECONDS)
    private void heartbeat() {
        AtomicInteger inProgress = new AtomicInteger();
        AtomicInteger inQueue = new AtomicInteger();
        AtomicInteger blocked = new AtomicInteger();

        synchronized (loadMap) {
            loadMap.values().forEach(v -> {
                switch (v) {
                    case IN_QUEUE:
                        inQueue.getAndIncrement();
                        break;

                    case IN_PROGRESS:
                        inProgress.getAndIncrement();
                        break;

                    case BLOCKED:
                        blocked.getAndIncrement();
                        break;

                    default:
                        break;
                }
            });
        }

        log.info("Load info: IN_PROGRESS - {}, IN_QUEUE - {}, BLOCKED - {}", inProgress, inQueue, blocked);

        log.info("Reload info: {}", reloadMap);
    }

    private enum LoadStatus {
        IN_QUEUE, IN_PROGRESS, BLOCKED
    }

    @Getter
    @Setter
    @ToString
    private static class ReloadData {
        private Double from;
        private LocalDateTime reloadTime;
    }
}
