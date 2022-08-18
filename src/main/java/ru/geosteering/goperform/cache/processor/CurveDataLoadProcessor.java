package ru.geosteering.goperform.cache.processor;

import io.nats.client.ConnectionListener;
import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.memcache.RealTimeCurveDataCache;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.model.event.*;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.service.EventBus;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class CurveDataLoadProcessor implements EventListener {

    private final Config config;
    private final MetaDataCache metaDataCache;
    private final RealTimeCurveDataCache realTimeCurveDataCache;
    private final Map<Long, LoadStatus> loadInfo = new ConcurrentHashMap<>();
    private final AtomicInteger requestAllowed = new AtomicInteger();
    private ScheduledExecutorService scheduler;

    @PostConstruct
    private void register() {
        EventBus.register(
                List.of(
                        Event.EventType.NEW_ACTIVE_CURVE,
                        Event.EventType.NATS_CONNECTION_STATUS,
                        Event.EventType.HISTORY_END_MESSAGE
                ),
                this);
    }

    @Override
    public void onEvent(Event<?> event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case NEW_ACTIVE_CURVE:
                onNewActiveCurve((NewActiveCurve) event);
                break;
            case NATS_CONNECTION_STATUS:
                onConnectionStatus((NatsConnectionStatus) event);
                break;
            case HISTORY_END_MESSAGE:
                onEndMessage((HistoryDataEndMessage) event);
                break;
            default:
                break;
        }
    }

    private void onNewActiveCurve(NewActiveCurve event) {
        loadInfo.putIfAbsent(event.getPayload(), LoadStatus.IN_QUEUE);
    }

    private void onConnectionStatus(NatsConnectionStatus event) {

        ConnectionListener.Events status = event.getPayload();

        if (status == ConnectionListener.Events.CONNECTED) {
            start();
        }

        if (status == ConnectionListener.Events.DISCONNECTED || status == ConnectionListener.Events.CLOSED) {
            stop();
        }
    }

    private void onEndMessage(HistoryDataEndMessage event) {

        HistoryDataEndMessage.LoadResult result = event.getPayload();
        Long curveId = event.getCurveId();

        if (result == HistoryDataEndMessage.LoadResult.DONE) {

            metaDataCache.addHistoryLoaded(curveId);

            if (metaDataCache.isActive(curveId)) {
                // TODO storage.mergeCache(id);
            }

            loadInfo.remove(curveId);

        } else {

            loadInfo.computeIfPresent(curveId, (k, v) -> LoadStatus.IN_QUEUE);
        }

        requestAllowed.incrementAndGet();
    }

    private void start() {
        loadInfo.entrySet()
                .forEach(entry -> entry.setValue(LoadStatus.IN_QUEUE));

        scheduler = Executors.newSingleThreadScheduledExecutor();

        requestAllowed.set(config.HISTORY_ONETIME_REQUESTS);

        scheduler.scheduleAtFixedRate(this::load, 30000, 100, TimeUnit.MILLISECONDS);
    }

    private void stop() {

        try {
            scheduler.shutdown();

            if (!scheduler.awaitTermination(1500, TimeUnit.MILLISECONDS)) {
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

                synchronized (loadInfo) {

                    loadInfo.entrySet().stream()
                            .filter(entry -> entry.getValue() == LoadStatus.IN_QUEUE)
                            .findFirst()
                            .ifPresent(entry -> {
                                entry.setValue(LoadStatus.IN_PROGRESS);
                                loadCurveData(entry.getKey());
                            });
                }

            } else {
                requestAllowed.incrementAndGet();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void loadCurveData(Long curveId) {
        String from = findFrom(curveId);
        String to = findTo(curveId);

        CurveDataRequest request = new CurveDataRequest(curveId, from, to, null, false, false, config.HISTORY_REQUEST_LIMIT, config.HISTORY_NUID + "." + curveId);

        Message message = NatsConnector.sendRequest(config.SUBJECT, StaticMapper.toBytes(request));

        if (message == null) {
            loadInfo.computeIfPresent(curveId, (k, v) -> LoadStatus.IN_QUEUE);
        }
    }

    private String findFrom(Long id) {

        CurveItem lastHistory = null; // TODO storage.getLastHistoryItem(id);
        MetaData metaData = metaDataCache.getMetaData(id);
        Double key = lastHistory == null ? metaData.getLastDBKey() : lastHistory.getKey();

        if (key == null) {
            return null;
        }

        return getKeyAsString(key, metaData.getIndexType());
    }

    private String findTo(Long id) {

        CurveItem firstReal = realTimeCurveDataCache.getFirst(id);
        Double key = firstReal == null ? null : firstReal.getKey();

        if (key == null) {
            return null;
        }

        return getKeyAsString(firstReal.getKey(), metaDataCache.getMetaData(id).getIndexType());
    }

    private String getKeyAsString(Double key, LogIndexType type) {

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

    private enum LoadStatus {
        IN_QUEUE, IN_PROGRESS
    }
}
