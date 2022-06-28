package ru.geosteering.goperform.cache.service;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.goperform.cache.controller.CacheResponse;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.model.ItemType;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.CurveCacheDTO;
import ru.geosteering.goperform.cache.repository.CurveCacheRepository;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.CacheUtils;

import javax.annotation.PreDestroy;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.geosteering.goperform.cache.config.Config.*;

@Component
@Slf4j
public class HistoryLoader {

    private final Storage storage;
    private final CurveCacheRepository repository;
    private final SimpMessagingTemplate wsTemplate;

    private final AtomicInteger requestAllowed;
    private final Map<Long, LoadStatus> loadInfo;

    private ScheduledExecutorService requestScheduler;

    @Setter
    private NatsConnector connector;

    public HistoryLoader(Storage storage, CurveCacheRepository repository, SimpMessagingTemplate wsTemplate) {
        this.storage = storage;
        this.repository = repository;
        this.wsTemplate = wsTemplate;
        requestAllowed = new AtomicInteger(HISTORY_ONETIME_REQUESTS);
        loadInfo = new ConcurrentHashMap<>();
    }

    public void start() {
        requestScheduler = Executors.newSingleThreadScheduledExecutor();
        requestScheduler.scheduleAtFixedRate(this::loadHistory, 30000, 100, TimeUnit.MILLISECONDS);

        log.info(getClass().getSimpleName() + " started");
    }

    public void restart() {
        requestAllowed.set(HISTORY_ONETIME_REQUESTS);
        loadInfo.clear();
        start();
    }

    private void loadHistory() {

        try {
            if (requestAllowed.getAndDecrement() > 0) {
                Long id = loadNext();

                if (id == null) {
                    requestAllowed.incrementAndGet();
                }

            } else {
                requestAllowed.incrementAndGet();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private Long loadNext() {

        Long id;

        synchronized (loadInfo) {

            id = loadInfo.entrySet().stream()
                    .filter(e -> e.getValue() == LoadStatus.WAIT)
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);

            if (id == null) {
                id = storage.getActiveCurves().stream()
                        .filter(curId -> !loadInfo.containsKey(curId))
                        .findFirst()
                        .orElse(null);

            }

            if (id != null) {
                applyStatus(id, LoadStatus.REQUEST);
            }
        }

        return id;
    }

    public void applyStatus(Long id, LoadStatus status) {

        log.info("Status for {}: {}", id, status);

        loadInfo.put(id, status);

        switch (status) {
            case REQUEST -> onStatusRequest(id);
            case ERROR -> onStatusError(id);
            case DONE -> onStatusDone(id);
            case PART -> onStatusPart(id);
            default -> {
            }
        }
    }

    private void onStatusRequest(Long id) {

        String from = getItemKeyAsString(storage.getLastHistory(id));

        String to = getItemKeyAsString(storage.getFirstReal(id));

        CurveDataRequest request = new CurveDataRequest(id, from, to, null, false, false, HISTORY_REQUEST_LIMIT, HISTORY_NUID + "." + id);
        log.info("Request: {}", request);

        try {
            connector.sendRequest(CacheUtils.toBytes(request));
        } catch (Exception e) {
            log.error("Send request exception: {}", e.getMessage(), e);
            applyStatus(id, LoadStatus.ERROR);
        }
    }

    private String getItemKeyAsString(CacheItem item) {
        String result = null;
        if (item != null) {
            Double key = item.getKey();
            if (item.getType() == ItemType.TIME) {
                result = OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
            } else {
                result = String.valueOf(key);
            }
        }

        return result;
    }

    private void onStatusError(Long id) {
        applyStatus(id, LoadStatus.WAIT);
    }

    private void onStatusDone(Long id) {
        storage.setHistoryLoaded(id);

        if (storage.isActiveCurve(id)) {
            storage.mergeCache(id);
        }
        //TODO websocket -> loaded
    }

    private void onStatusPart(Long id) {
        applyStatus(id, LoadStatus.WAIT);
    }

    public void onEndMessage() {
        requestAllowed.incrementAndGet();
    }

    public CacheResponse getCacheResponse(Long id, Double from, Double to) {

        if (storage.isHistoryLoaded(id)) {
            log.info("Begin response preparing for id {}", id);
            Double finalFrom = from == null ? 0 : from;
            Double finalTo = to == null ? Double.MAX_VALUE : to;

            List<CacheItem> items = new ArrayList<>();

            List<List<CacheItem>> fromDB = repository.get(id, finalFrom, finalTo).stream()
                    .map(CurveCacheDTO::toItems)
                    .toList();

            if (!fromDB.isEmpty()) {
                fromDB.get(0).removeIf(i -> i.getKey() < finalFrom);
                fromDB.get(fromDB.size() - 1).removeIf(i -> i.getKey() > finalTo);
                fromDB.forEach(items::addAll);
            }

            items.addAll(storage.getFromCache(id, finalFrom, finalTo));

            CacheResponse response = new CacheResponse();
            response.setId(id);
            response.setData(items);
            log.info("Response for id {} prepared. Items count: {}", id, items.size());
            return response;
        }

        if (!loadInfo.containsKey(id)) {
            applyStatus(id, LoadStatus.WAIT);
        }

        return null;
    }

    @PreDestroy
    public void stop() {
        try {
            requestScheduler.shutdown();
            if (!requestScheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                log.warn(getClass().getSimpleName() + " requestScheduler shutdown timeout");
                requestScheduler.shutdownNow();
            }
            log.info(getClass().getSimpleName() + " requestScheduler shutdown");
        } catch (Exception e) {
            log.error(getClass().getSimpleName() + "requestScheduler shutdown exception: {}", e.getMessage(), e);
        }
    }
}
