package ru.geosteering.goperform.cache.processor;

import io.nats.client.ConnectionListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.HistoryCurveDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.EventListener;
import ru.geosteering.goperform.cache.model.event.*;
import ru.geosteering.goperform.cache.service.EventBus;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Component
@RequiredArgsConstructor
@Slf4j
public class HistoryApiMessageProcessor implements EventListener {

    private final HistoryCurveDataCache historyCache;
    private final Config config;
    private final Map<Long, AtomicInteger> receivedCount = new ConcurrentHashMap<>();
    private final Map<Long, Set<CurveItem>> buffer = new ConcurrentHashMap<>();

    @PostConstruct
    private void register() {
        EventBus.register(List.of(Event.EventType.HISTORY_API_MESSAGE, Event.EventType.NATS_CONNECTION_STATUS), this);
    }

    @Override
    public void onEvent(Event<?> event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case HISTORY_API_MESSAGE:
                onApiMessage((HistoryApiMessage) event);
                break;
            case NATS_CONNECTION_STATUS:
                onConnectionStatus((NatsConnectionStatus) event);
                break;
            default:
                break;
        }
    }

    private void onConnectionStatus(NatsConnectionStatus event) {

        ConnectionListener.Events status = event.getPayload();

        if (status == ConnectionListener.Events.DISCONNECTED || status == ConnectionListener.Events.CLOSED) {

            Executors.newSingleThreadScheduledExecutor()
                    .schedule(() -> {
                        receivedCount.clear();
                        buffer.clear();
                        historyCache.clearAll();
                    }, 2000, TimeUnit.MILLISECONDS);
        }
    }

    private void onApiMessage(HistoryApiMessage event) {

        ApiMessage apiMessage = event.getPayload();
        ApiMessage.MessageType type = apiMessage.getType();

        switch (type) {
            case CURVE_DATA:
                processCurveData((CurveDataMessage) apiMessage);
                break;
            case DATA_END:
                processDataEnd((DataEndMessage) apiMessage, event.getSubject());
                break;
            default:
                log.info("Some apiMessage: {}", apiMessage);
                break;
        }
    }

    private void processCurveData(CurveDataMessage curveDataMessage) {

        CurveItem item = CurveItem.fromAbstractDataItem(curveDataMessage.getData());
        Long curveId = curveDataMessage.getId();

        buffer.computeIfAbsent(curveId, key -> ConcurrentHashMap.newKeySet(config.HISTORY_REQUEST_LIMIT))
                .add(item);

        receivedCount.computeIfAbsent(curveId, key -> new AtomicInteger(0))
                .incrementAndGet();

        EventBus.post(new NewCurveItem(curveId, item));

    }

    private void processDataEnd(DataEndMessage dataEndMessage, String subject) {

        Long id = parseId(subject);

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));

        int sent = dataEndMessage.getSentCount();
        int received = receivedCount.containsKey(id) ? receivedCount.remove(id).get() : -1;

        if (sent == 0) {
            log.info("Data loaded: id {}, message {}", id, dataEndMessage);
            EventBus.post(new HistoryDataEndMessage(id, HistoryDataEndMessage.LoadResult.DONE));

        } else if (sent != received) {
            log.error("Received count '{}' not equals to sent '{}'", received, sent);
            EventBus.post(new HistoryDataEndMessage(id, HistoryDataEndMessage.LoadResult.ERROR));

        } else {
            log.info("History part received: id {}, message {}", id, dataEndMessage);
            drainToCache(id);
            EventBus.post(new HistoryDataEndMessage(id, HistoryDataEndMessage.LoadResult.PART));
        }

        buffer.remove(id);
    }

    private Long parseId(String subject) {

        try {
            String[] arr = subject.split("\\.");
            return Long.parseLong(arr[arr.length - 1]);
        } catch (Exception e) {
            log.error("Parsing id from subject exception: {}", subject, e);
            return null;
        }
    }

    private void drainToCache(Long id) {
        log.info("Drain buffer to historyCache. Curve id: {}, items: {}", id, buffer.get(id).size());
        historyCache.add(id, buffer.get(id));
    }
}
