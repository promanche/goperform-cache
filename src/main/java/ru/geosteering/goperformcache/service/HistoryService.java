package ru.geosteering.goperformcache.service;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperformcache.model.*;
import ru.geosteering.goperformcache.nats.NatsConnector;
import ru.geosteering.goperformcache.repository.MetaDataDTO;
import ru.geosteering.goperformcache.repository.MyBatisRepository;
import ru.geosteering.goperformcache.storage.Storage;
import ru.geosteering.goperformcache.utils.CacheUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.geosteering.goperformcache.config.Config.*;

@Component
@Slf4j
public class HistoryService implements MessageHandler {

    private final Storage storage;
    private final MyBatisRepository repository;

    private final AtomicInteger requestAllowed;
    private final Map<Long, MetaDataDTO> metaData;
    private final Map<Long, AtomicInteger> receivedCount;
    private final Map<Long, LoadStatus> loadInfo;
    private final Map<Long, Set<CurveDataItem>> buffer;

    private ExecutorService messageHandler;
    private ScheduledExecutorService requestScheduler;

    @Setter
    private NatsConnector connector;

    public HistoryService(Storage storage, MyBatisRepository repository) {
        this.storage = storage;
        this.repository = repository;
        requestAllowed = new AtomicInteger(HISTORY_ONE_TIME_REQUESTS);
        metaData = new HashMap<>();
        receivedCount = new HashMap<>();
        loadInfo = new HashMap<>();
        buffer = new HashMap<>();
    }

    @PostConstruct
    private void start() {

        loadMetaData();
        initExecutors();

        log.info(getClass().getSimpleName() + " started");
    }

    public void restart() {
        requestAllowed.set(HISTORY_ONE_TIME_REQUESTS);
        metaData.clear();
        receivedCount.clear();
        loadInfo.clear();
        buffer.clear();

        storage.onRestartHistory();

        start();
    }

    private void loadMetaData() {
        List<MetaDataDTO> metaData = repository.getMetaData();
        metaData.forEach(data -> this.metaData.put(data.getCurveId(), data));
        log.info("Metadata loaded: {}", metaData);
    }

    private void initExecutors() {
        messageHandler = Executors.newFixedThreadPool(HISTORY_THREADS);
        requestScheduler = Executors.newSingleThreadScheduledExecutor();

        requestScheduler.scheduleWithFixedDelay(this::loadHistory, 20, 1, TimeUnit.SECONDS);
    }

    private void loadHistory() {

        if (requestAllowed.getAndDecrement() > 0) {
            Long id = findForLoad();

            if (id == null) {
                requestAllowed.incrementAndGet();
            } else {
                sendRequest(id);
            }

        } else {
            requestAllowed.incrementAndGet();
        }
    }

    private Long findForLoad() {

        Long id;

        synchronized (loadInfo) {

            id = loadInfo.entrySet().stream()
                    .filter(e -> e.getValue() == LoadStatus.PARK)
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

    private void sendRequest(Long id) {

        String from = metaData.get(id) == null ?
                null : metaData.get(id).getLast().plusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME);

        OffsetDateTime firstReal = storage.getFirstReal(id);

        String to = firstReal == null ?
                null : firstReal.minusSeconds(1).format(DateTimeFormatter.ISO_DATE_TIME);

        CurveDataRequest request = new CurveDataRequest(id, from, to, null, false, false, HISTORY_REQUEST_LIMIT, HISTORY_NUID + "." + id);
        log.info("Request: {}", request);

        receivedCount.put(id, new AtomicInteger(0));

        try {
            connector.sendRequest(request.toBytes());
        } catch (ExecutionException | InterruptedException e) {
            log.error("Send request exception: {}", e.getMessage(), e);
            applyStatus(id, LoadStatus.ERROR);
        }
    }

    @Override
    public void onMessage(Message msg) {
        messageHandler.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {

        String json = new String(msg.getData());

        CurveDataMessage curveDataMessage = CacheUtils.parseCurveDataMessage(json, msg.getSubject());

        if (curveDataMessage == null) {
            Long id = CacheUtils.getIdFromSubject(msg.getSubject());
            if (id == null) {
                return;
            }

            if (CacheUtils.getFieldFromJson(json, "type").equalsIgnoreCase("end")) {
                String sent = CacheUtils.getFieldFromJson(json, "sentCount");

                if (sent.equalsIgnoreCase("0")) {
                    applyStatus(id, LoadStatus.DONE);

                } else if (!sent.equals(String.valueOf(receivedCount.get(id).get()))) {
                    log.error("Received count '{}' not equals to sent '{}'", receivedCount.get(id).get(), sent);
                    applyStatus(id, LoadStatus.ERROR);

                } else {
                    log.info("History part received: subject {}, message {}", msg.getSubject(), json);
                    drainToStorage(id);
                    refreshMetaData(id);
                    applyStatus(id, LoadStatus.PARK);
                }

                requestAllowed.incrementAndGet();
            }

        } else {
            buffer.computeIfAbsent(curveDataMessage.getId(), v -> ConcurrentHashMap.newKeySet(HISTORY_REQUEST_LIMIT))
                    .add(curveDataMessage.getData());

//            storage.addData(curveDataMessage.getId(), curveDataMessage.getData(), false);
            receivedCount.get(curveDataMessage.getId()).incrementAndGet();
        }
    }

    private void applyStatus(Long id, LoadStatus status) {

        log.info("Status for {}: {}", id, status);

        loadInfo.put(id, status);

        switch (status) {
            case ERROR -> onStatusError(id);
            case DONE -> onStatusDone(id);
            default -> {
            }
        }
    }

    private void onStatusError(Long id) {
        buffer.get(id).clear();
        log.info("Clear buffer for id {}", id);
        applyStatus(id, LoadStatus.PARK);
    }

    private void onStatusDone(Long id) {
        storage.setHistoryLoaded(id);

        if (storage.isActiveCurve(id)) {
            storage.mergeCache(id);
        }
        //TODO websocket -> loaded
    }

    private void drainToStorage(Long id) {

        log.info("Drain buffer to storage. Curve id: {}, items: {}", id, buffer.get(id).size());

        Iterator<CurveDataItem> iterator = buffer.get(id).iterator();
        while (iterator.hasNext()) {
            storage.addData(id, iterator.next(), false);
            iterator.remove();
        }
    }

    private void refreshMetaData(Long id) {

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

    private void shutdownHandler() {
        try {
            messageHandler.shutdown();
            if (!messageHandler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn(getClass().getSimpleName() + " messageHandler shutdown timeout");
                messageHandler.shutdownNow();
            }
            log.info(getClass().getSimpleName() + " messageHandler shutdown");
        } catch (Exception e) {
            log.error(getClass().getSimpleName() + "messageHandler shutdown exception: {}", e.getMessage(), e);
        }
    }

    private void shutdownScheduler() {
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

    @PreDestroy
    public void stop() {
        shutdownScheduler();
        shutdownHandler();
    }

    private enum LoadStatus {
        DONE, REQUEST, PARK, ERROR
    }
}
