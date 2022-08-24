package ru.geosteering.goperform.cache.processor;

import io.nats.client.ConnectionListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.HistoryDataCache;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.Event;
import ru.geosteering.goperform.cache.model.event.apimessage.HistoryApiMessage;
import ru.geosteering.goperform.cache.model.event.apimessage.DataEndMessage;
import ru.geosteering.goperform.cache.model.event.item.HistoryItem;
import ru.geosteering.goperform.cache.model.event.item.RealtimeItem;
import ru.geosteering.goperform.cache.model.event.task.ClearTask;
import ru.geosteering.goperform.cache.model.event.natsconnection.NatsConnectionStatus;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class HistoryDataProcessor implements EventProcessor {

    private final HistoryDataCache historyCache;
    private final MetaDataCache metaDataCache;
    private final Config config;
    private final EventBus eventBus;

    private final Map<Long, AtomicInteger> receivedCount = new ConcurrentHashMap<>();
    private final Map<Long, Set<CurveItem>> buffer = new ConcurrentHashMap<>();

    @PostConstruct
    private void register() {
        eventBus.register(
                List.of(
                        Event.EventType.HISTORY_API_MESSAGE,
                        Event.EventType.NATS_CONNECTION_STATUS,
                        Event.EventType.CLEAR_TASK
                ),
                this);
    }

    @Override
    public void processEvent(Event event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case HISTORY_API_MESSAGE:
                onApiMessage((HistoryApiMessage) event);
                break;
            case NATS_CONNECTION_STATUS:
                onConnectionStatus((NatsConnectionStatus) event);
                break;
            case CLEAR_TASK:
                historyCache.remove(((ClearTask) event).getId());
                break;
            default:
                break;
        }
    }

    private void onConnectionStatus(NatsConnectionStatus event) {

        ConnectionListener.Events status = event.getStatus();

        if (status == ConnectionListener.Events.DISCONNECTED || status == ConnectionListener.Events.CLOSED) {

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            receivedCount.clear();
            buffer.clear();
            historyCache.removeAll();

            //TODO handler stop event?
        }
    }

    private void onApiMessage(HistoryApiMessage event) {

        ApiMessage apiMessage = event.getApiMessage();
        ApiMessage.MessageType type = apiMessage.getType();

        switch (type) {
            case CURVE_DATA:
                processCurveData((CurveDataMessage) apiMessage);
                break;
            case DATA_END:
                processDataEnd((ru.geosteering.commonModels.dataService.responses.DataEndMessage) apiMessage, event.getSubject());
                break;
            default:
                log.info("Some apiMessage: {}", apiMessage);
                break;
        }
    }

    private void processCurveData(CurveDataMessage curveDataMessage) {

        Long id = curveDataMessage.getId();

        CurveItem item = CurveItem.fromAbstractDataItem(curveDataMessage.getData(),
                metaDataCache.getMetaData(id).getIndexType() != LogIndexType.MEASURED_DEPTH);

        buffer.computeIfAbsent(id, key -> ConcurrentHashMap.newKeySet(config.HISTORY_REQUEST_LIMIT))
                .add(item);

        receivedCount.computeIfAbsent(id, key -> new AtomicInteger(0))
                .incrementAndGet();

        eventBus.post(new HistoryItem(id, item));

    }

    private void processDataEnd(ru.geosteering.commonModels.dataService.responses.DataEndMessage dataEndMessage, String subject) {

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        Long id = parseId(subject);

        int sent = dataEndMessage.getSentCount();
        int received = receivedCount.containsKey(id) ? receivedCount.remove(id).get() : -1;

        if (sent == 0) {
            log.info("{} data loaded, {}", id, dataEndMessage);
            eventBus.post(new DataEndMessage(id, DataEndMessage.Result.DONE));

        } else if (sent != received) {
            log.error("Received count {} not equals to sent {}", received, sent);
            eventBus.post(new DataEndMessage(id, DataEndMessage.Result.ERROR));

        } else {
            log.info("{} history part received, {}", id, dataEndMessage);
            drainToCache(id);
            eventBus.post(new DataEndMessage(id, DataEndMessage.Result.PART));
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
