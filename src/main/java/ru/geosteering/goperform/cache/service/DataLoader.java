package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PreDestroy;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class DataLoader {

    private final Storage storage;
    private final SimpMessagingTemplate wsTemplate;
    private final Config config;
    private final MainRepository repository;
    private final Approximator approximator;
    private final MetaDataProcessor metaDataProcessor;

    private final AtomicInteger requestAllowed;
    private final Map<Long, LoadStatus> loadInfo;
    private final Map<Long, ReloadData> reloadInfo;

    private ScheduledExecutorService requestScheduler;

    public DataLoader(Storage storage, SimpMessagingTemplate wsTemplate, Config config, MainRepository repository, Approximator approximator, MetaDataProcessor metaDataProcessor) {
        this.storage = storage;
        this.wsTemplate = wsTemplate;
        this.config = config;
        this.repository = repository;
        this.approximator = approximator;
        this.metaDataProcessor = metaDataProcessor;

        requestAllowed = new AtomicInteger(config.HISTORY_ONETIME_REQUESTS);
        loadInfo = new ConcurrentHashMap<>();
        reloadInfo = new HashMap<>();
    }

    //======================= Start / Restart =======================

    public void start() {
        requestScheduler = Executors.newSingleThreadScheduledExecutor();
        requestScheduler.scheduleAtFixedRate(this::loadHistory, 30000, 100, TimeUnit.MILLISECONDS);

        log.info(getClass().getSimpleName() + " started");
    }

    public void restart() {
        requestAllowed.set(config.HISTORY_ONETIME_REQUESTS);

        Map<Long, LoadStatus> loadInfoCopy = new HashMap<>(loadInfo);
        loadInfo.clear();
        loadInfoCopy.forEach((k, v) -> {
            if (v == LoadStatus.REQUEST) {
                applyStatus(k, LoadStatus.WAIT);
            } else {
                applyStatus(k, v);
            }
        });

        start();
    }

    //======================= Data loading =======================

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
                        .filter(curId -> !storage.isHistoryLoaded(curId) && !loadInfo.containsKey(curId))
                        .findFirst()
                        .orElse(null);

            }

            if (id != null) {
                applyStatus(id, LoadStatus.REQUEST);
            }
        }

        return id;
    }


    private void applyStatus(Long id, LoadStatus status) {
        applyStatus(id, status, false);
    }

    private void applyStatus(Long id, LoadStatus status, boolean rewriteBlocked) {

        if (rewriteBlocked || loadInfo.get(id) != LoadStatus.BLOCKED) {
            log.debug("Status for {}: {}", id, status);

            loadInfo.put(id, status);

            switch (status) {
                case REQUEST:
                    onStatusRequest(id);
                    break;
                case DONE:
                    onStatusDone(id);
                    break;
                default:
                    break;
            }
        } else {
            log.debug("Curve {} blocked", id);
        }
    }

    private void onStatusRequest(Long id) {

        String from = findFrom(id);
        String to = findTo(id);

        CurveDataRequest request = new CurveDataRequest(id, from, to, null, false, false, config.HISTORY_REQUEST_LIMIT, config.HISTORY_NUID + "." + id);

        Message message = NatsConnector.sendRequest(config.SUBJECT, StaticMapper.toBytes(request));

        if (message == null) {
            applyStatus(id, LoadStatus.WAIT);
        }
    }

    private String findFrom(Long id) {

        CurveItem lastHistory = storage.getLastHistoryItem(id);

        if (lastHistory != null) {
            return getKeyAsString(lastHistory.getKey(), metaDataProcessor.getIndexType(id));
        } else {
            MetaData metaData = metaDataProcessor.getMetaData(id);
            return getKeyAsString(metaData.getLastDBKey(), metaData.getIndexType());
        }
    }

    private String findTo(Long id) {

        CurveItem firstReal = storage.getFirstRealItem(id);

        if (firstReal != null) {
            return getKeyAsString(firstReal.getKey(), metaDataProcessor.getIndexType(id));
        }

        return null;
    }

    private String getKeyAsString(Double key, LogIndexType type) {

        if (key == null) {
            return null;
        }

        switch (type) {
            case DATE_TIME: {
                return OffsetDateTime.ofInstant(Instant.ofEpochMilli(key.longValue()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME);
            }
            case VERTICAL_DEPTH:
            case MEASURED_DEPTH: {
                return String.valueOf(key);
            }
            default: {
                return null;
            }
        }
    }

    private void onStatusDone(Long id) {
        storage.setHistoryLoaded(id);

        if (storage.isActiveCurve(id)) {
            storage.mergeCache(id);
        }
        wsTemplate.convertAndSend("/curve/" + id + "/loaded", "{\"curveId\":" + id + "}");

        loadInfo.remove(id);
    }

    public void onEndMessage() {
        requestAllowed.incrementAndGet();
    }

    private enum LoadStatus {
        DONE, REQUEST, WAIT, BLOCKED
    }

    //======================= From history handler =======================

    public void onLoadFull(Long id) {
        applyStatus(id, LoadStatus.DONE);
    }

    public void onLoadPart(Long id) {
        applyStatus(id, LoadStatus.WAIT);
    }

    public void onLoadError(Long id) {
        applyStatus(id, LoadStatus.WAIT);
    }

    //======================= From rest service =======================

    public void loadByRequest(Long id) {
        if (!loadInfo.containsKey(id)) {
            applyStatus(id, LoadStatus.WAIT);
        }
    }

    //======================= Data reloading =======================

    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.SECONDS)
    private void checkReloadData() {

        synchronized (reloadInfo) {

            if (!reloadInfo.isEmpty()) {

                LocalDateTime now = LocalDateTime.now();

                reloadInfo.forEach((id, data) -> {
                    if (data.getReloadTime().isBefore(now)) {
                        reload(id, data.getFrom());
                    }
                });
            }
        }
    }

    private void reload(Long id, Double from) {
        log.info("Run reload process for curve {} from {}", id, from);

        storage.resetById(id);
        approximator.resetById(id);

        repository.deleteItems(id, from);
        repository.deleteSegments(id, from);

        metaDataProcessor.reloadById(id);

        approximator.loadLostById(id);

        reloadInfo.remove(id);

        applyStatus(id, LoadStatus.WAIT, true);
    }

    public void addForReload(Long id, Double from, int delaySeconds) {

        applyStatus(id, LoadStatus.BLOCKED);

        synchronized (reloadInfo) {

            if (reloadInfo.containsKey(id)) {

                ReloadData reloadData = reloadInfo.get(id);
                if (from < reloadData.getFrom()) {
                    reloadData.setFrom(from);
                }
                reloadData.setReloadTime(LocalDateTime.now().plusSeconds(delaySeconds));

            } else {

                ReloadData reloadData = new ReloadData();
                reloadData.setFrom(from);
                reloadData.setReloadTime(LocalDateTime.now().plusSeconds(delaySeconds));
                reloadInfo.put(id, reloadData);
            }
        }
    }

    @Getter
    @Setter
    private static class ReloadData {
        private Double from;
        private LocalDateTime reloadTime;
    }

    //======================= Stop =======================

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
