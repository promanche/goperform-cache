package ru.geosteering.goperform.cache.service;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.CurveDataItem;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.goperform.cache.repository.dto.CurveCacheDTO;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.controller.dto.CacheResponse;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.CurveCacheRepository;
import ru.geosteering.goperform.cache.repository.dto.MetaDataDTO;
import ru.geosteering.goperform.cache.utils.CacheUtils;

import javax.annotation.PreDestroy;
import java.time.OffsetDateTime;
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
        requestScheduler.scheduleAtFixedRate(this::loadHistory, 30000, 100, TimeUnit.MILLISECONDS);

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

        String from = metaData.get(id) == null ?
                null : metaData.get(id).getLast().plusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME);

        OffsetDateTime firstReal = storage.getFirstReal(id);

        String to = firstReal == null ?
                null : firstReal.minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME);

        CurveDataRequest request = new CurveDataRequest(id, from, to, null, false, false, HISTORY_REQUEST_LIMIT, HISTORY_NUID + "." + id);
        log.info("Request: {}", request);

        try {
            connector.sendRequest(CacheUtils.toBytes(request));
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

    private void onStatusPart(Long id) {
        refreshMetaData(id);
        applyStatus(id, LoadStatus.WAIT);
    }

    private void refreshMetaData(Long id) {

        MetaDataDTO metaDataDTO = metaData.get(id);

        if (metaDataDTO == null) {
            metaDataDTO = new MetaDataDTO();
            OffsetDateTime first = repository.getMinFirst(id);
            metaDataDTO.setCurveId(id);
            metaDataDTO.setFirst(first);
        }

        OffsetDateTime last = storage.getLastHistory(id);
        metaDataDTO.setLast(last);

        metaData.put(id, metaDataDTO);
    }

    public void onEndMessage() {
        requestAllowed.incrementAndGet();
    }

    public CacheResponse getCacheResponse(Long id, OffsetDateTime from, OffsetDateTime to) {

        if (storage.isHistoryLoaded(id)) {
            OffsetDateTime finalFrom = from == null ? metaData.get(id).getFirst() : from;
            OffsetDateTime finalTo = to == null ? OffsetDateTime.now().plusHours(23) : to;

            List<CurveDataItem> items = new ArrayList<>();

            List<List<CurveDataItem>> fromDB = repository.get(id, finalFrom, finalTo).stream()
                    .map(CurveCacheDTO::toItems).toList();

            if (!fromDB.isEmpty()) {
                fromDB.get(0).removeIf(i -> i.time.isBefore(finalFrom));
                fromDB.get(fromDB.size() - 1).removeIf(i -> i.time.isAfter(finalTo));
                fromDB.forEach(items::addAll);
            }

            items.addAll(storage.getFromCache(id, finalFrom, finalTo));

            CacheResponse response = new CacheResponse();
            response.setId(id);
            response.setData(items);
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
