package ru.geosteering.goperform.cache.processor;

import io.nats.client.ConnectionListener;
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
import ru.geosteering.goperform.cache.model.event.Event;
import ru.geosteering.goperform.cache.model.event.apimessage.DataEndMessage;
import ru.geosteering.goperform.cache.model.event.curve.NewActiveCurve;
import ru.geosteering.goperform.cache.model.event.natsconnection.NatsConnectionStatus;
import ru.geosteering.goperform.cache.model.event.task.*;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
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
public class CurveDataLoadProcessor implements EventProcessor {

    private final Config config;
    private final MetaDataCache metaDataCache;
    private final RealtimeDataCache realtimeDataCache;
    private final EventBus eventBus;
    private final HistoryDataCache historyDataCache;
    private final MainRepository repository;

    private final Map<Long, LoadStatus> loadMap = new ConcurrentHashMap<>();
    private final Map<Long, ReloadData> reloadMap = new ConcurrentHashMap<>();
    private final AtomicInteger requestAllowed = new AtomicInteger();
    private ScheduledExecutorService scheduler;

    @PostConstruct
    private void register() {
        eventBus.register(
                List.of(
                        Event.EventType.NEW_ACTIVE_CURVE,
                        Event.EventType.NATS_CONNECTION_STATUS,
                        Event.EventType.DATA_END_MESSAGE,
                        Event.EventType.RELOAD_TASK,
                        Event.EventType.LOAD_TASK
                ),
                this);
    }

    @Override
    public void processEvent(Event event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case NEW_ACTIVE_CURVE:
                addForLoad(((NewActiveCurve) event).getId());
                break;
            case LOAD_TASK:
                addForLoad(((LoadITask) event).getId());
                break;
            case NATS_CONNECTION_STATUS:
                onConnectionStatus((NatsConnectionStatus) event);
                break;
            case DATA_END_MESSAGE:
                onEndMessage((DataEndMessage) event);
                break;
            case RELOAD_TASK:
                addForReload((ReloadTask) event);
                break;
            default:
                break;
        }
    }

    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.SECONDS)
    public void reload() {

        if (!reloadMap.isEmpty()) {

            LocalDateTime now = LocalDateTime.now();

            Set.copyOf(reloadMap.keySet()).forEach(id -> {

                ReloadData reloadData = reloadMap.remove(id);

                if (reloadData.getReloadTime().isBefore(now)) {

                    repository.deleteItems(id, reloadData.getFrom());
                    repository.deleteSegments(id, reloadData.getFrom());

                    Future<?> future = eventBus.post(new ClearTask(id));

                    try {
                        future.get();
                        loadMap.put(id, LoadStatus.IN_QUEUE);
                    } catch (InterruptedException | ExecutionException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });
        }
    }

    private void addForLoad(Long id) {
        loadMap.putIfAbsent(id, LoadStatus.IN_QUEUE);
    }

    private void onConnectionStatus(NatsConnectionStatus event) {

        ConnectionListener.Events status = event.getStatus();

        if (status == ConnectionListener.Events.CONNECTED) {
            start();
        }

        if (status == ConnectionListener.Events.DISCONNECTED || status == ConnectionListener.Events.CLOSED) {
            stop();
        }
    }

    private void onEndMessage(DataEndMessage event) {

        DataEndMessage.Result result = event.getResult();
        Long id = event.getId();

        if (result == DataEndMessage.Result.DONE) {

            metaDataCache.addHistoryLoaded(id);

            if (metaDataCache.isActive(id)) {
                List<CurveItem> items = historyDataCache.drain(id);
                realtimeDataCache.merge(id, items);
            }

            loadMap.computeIfPresent(id, (aLong, loadStatus) -> loadStatus == LoadStatus.BLOCKED ? loadStatus : null);

        } else {

            loadMap.computeIfPresent(id, (aLong, loadStatus) -> loadStatus == LoadStatus.BLOCKED ? loadStatus : LoadStatus.IN_QUEUE);
        }

        requestAllowed.incrementAndGet();
    }

    private void addForReload(ReloadTask event) {

        loadMap.put(event.getId(), LoadStatus.BLOCKED);

        Double from = event.getFrom();
        LocalDateTime reloadTime = LocalDateTime.now().plusMinutes(5);

        reloadMap.compute(event.getId(), (aLong, reloadData) -> {

            if (reloadData != null) {
                reloadData.setReloadTime(reloadTime);
                if (Double.compare(reloadData.getFrom(), from) > 0) {
                    reloadData.setFrom(from);
                }
                return reloadData;

            } else {
                ReloadData newData = new ReloadData();
                newData.setReloadTime(reloadTime);
                newData.setFrom(from);
                return newData;
            }
        });
    }

    private void start() {
        loadMap.entrySet()
                .forEach(entry -> {
                    if (entry.getValue() != LoadStatus.BLOCKED) {
                        entry.setValue(LoadStatus.IN_QUEUE);
                    }
                });

        scheduler = Executors.newSingleThreadScheduledExecutor();

        requestAllowed.set(config.HISTORY_ONETIME_REQUESTS);

        scheduler.scheduleAtFixedRate(this::load, 30000, 100, TimeUnit.MILLISECONDS);
    }

    private void stop() {

        try {
            scheduler.shutdown();

            if (!scheduler.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                log.warn(getClass().getSimpleName() + " scheduler shutdown timeout");
                scheduler.shutdownNow();
            }

            log.info(getClass().getSimpleName() + " scheduler shutdown");

        } catch (InterruptedException e) {
            log.error(getClass().getSimpleName() + "scheduler shutdown exception: {}", e.getMessage(), e);
        }
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

    private enum LoadStatus {
        IN_QUEUE, IN_PROGRESS, BLOCKED
    }

    @Getter
    @Setter
    private static class ReloadData {
        private Double from;
        private LocalDateTime reloadTime;
    }
}
