package ru.geosteering.goperformcache.service;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import ru.geosteering.goperformcache.controller.dto.CacheResponse;
import ru.geosteering.goperformcache.model.CurveDataRequest;
import ru.geosteering.goperformcache.nats.NatsConnector;
import ru.geosteering.goperformcache.repository.CurveCacheRepository;
import ru.geosteering.goperformcache.repository.dto.MetaDataDTO;
import ru.geosteering.goperformcache.storage.Storage;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.geosteering.goperformcache.config.Config.*;

@Component
@Slf4j
public class HistoryLoader {

    private final Storage storage;
    private final CurveCacheRepository repository;
    private final SimpMessagingTemplate wsTemplate;

    private final AtomicInteger requestAllowed;
    private final Map<Long, MetaDataDTO> metaData;
    private final Map<Long, LoadStatus> loadInfo;

    private ScheduledExecutorService requestScheduler;

    @Setter
    private NatsConnector connector;

    public HistoryLoader(Storage storage, CurveCacheRepository repository, SimpMessagingTemplate wsTemplate) {
        this.storage = storage;
        this.repository = repository;
        this.wsTemplate = wsTemplate;
        requestAllowed = new AtomicInteger(HISTORY_ONETIME_REQUESTS);
        metaData = new ConcurrentHashMap<>();
        loadInfo = new ConcurrentHashMap<>();
    }

    public void start() {

        loadMetaData();

        requestScheduler = Executors.newSingleThreadScheduledExecutor();
        requestScheduler.scheduleWithFixedDelay(this::loadHistory, 30000, 200, TimeUnit.MILLISECONDS);

        log.info(getClass().getSimpleName() + " started");
    }

    public void restart() {
        requestAllowed.set(HISTORY_ONETIME_REQUESTS);
        metaData.clear();
        loadInfo.clear();
        start();
    }

    private void loadMetaData() {
        List<MetaDataDTO> metaData = repository.getMetaData();
        metaData.forEach(data -> this.metaData.put(data.getCurveId(), data));

        log.info("Metadata loaded: {}", metaData);
    }

    private void loadHistory() {

        if (requestAllowed.getAndDecrement() > 0) {
            Long id = loadNext();

            if (id == null) {
                requestAllowed.incrementAndGet();
            }

        } else {
            requestAllowed.incrementAndGet();
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
            default -> {
            }
        }
    }

    private void onStatusRequest(Long id) {

        String from = metaData.get(id) == null ?
                null : metaData.get(id).getLast().plusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME);

        OffsetDateTime firstReal = storage.getFirstReal(id);

        String to = firstReal == null ?
                null : firstReal.minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME);

        CurveDataRequest request = new CurveDataRequest(id, from, to, null, false, false, HISTORY_REQUEST_LIMIT, HISTORY_NUID + "." + id);
        log.info("Request: {}", request);

        try {
            connector.sendRequest(request.toBytes());
        } catch (Exception e) {
            log.error("Send request exception: {}", e.getMessage());
            applyStatus(id, LoadStatus.ERROR);
        }
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

    public void refreshMetaData(Long id) {

        OffsetDateTime last = storage.getLastHistory(id);
        OffsetDateTime first = storage.getFirstHistory(id);

        MetaDataDTO metaDataDTO = metaData.get(id);
        if (metaDataDTO == null) {
            metaDataDTO = new MetaDataDTO();
            metaDataDTO.setCurveId(id);
        }

        metaDataDTO.setFirst(first);
        metaDataDTO.setLast(last);

        metaData.put(id, metaDataDTO);
    }

    public void onEndMessage() {
        requestAllowed.incrementAndGet();
    }

    public CacheResponse getCacheResponse(Long id, OffsetDateTime from, OffsetDateTime to) {

        if (storage.isHistoryLoaded(id)) {
            return new CacheResponse(); //TODO not realized yet
        }

        if (!loadInfo.containsKey(id)) {
            applyStatus(id, LoadStatus.WAIT);
        }

        return null;
    }

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
