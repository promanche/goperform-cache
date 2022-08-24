package ru.geosteering.goperform.cache.processor;

import io.nats.client.ConnectionListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.memcache.RealtimeDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.Event;
import ru.geosteering.goperform.cache.model.event.apimessage.RealtimeApiMessage;
import ru.geosteering.goperform.cache.model.event.item.*;
import ru.geosteering.goperform.cache.model.event.natsconnection.NatsConnectionStatus;
import ru.geosteering.goperform.cache.model.event.task.ClearTask;
import ru.geosteering.goperform.cache.model.event.task.ReloadTask;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataProcessor implements EventProcessor {

    private final MetaDataCache metaDataCache;
    private final RealtimeDataCache realTimeCache;
    private final EventBus eventBus;

    @PostConstruct
    private void register() {
        eventBus.register(
                List.of(
                        Event.EventType.REALTIME_API_MESSAGE,
                        Event.EventType.NATS_CONNECTION_STATUS,
                        Event.EventType.CLEAR_TASK
                ),
                this);
    }

    @Override
    public void processEvent(Event event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case REALTIME_API_MESSAGE:
                onApiMessage((RealtimeApiMessage) event);
                break;
            case NATS_CONNECTION_STATUS:
                onConnectionStatus((NatsConnectionStatus) event);
                break;
            case CLEAR_TASK:
                realTimeCache.remove(((ClearTask) event).getId());
                break;
            default:
                break;
        }
    }

    private void onApiMessage(RealtimeApiMessage event) {

        ApiMessage apiMessage = event.getApiMessage();

        if (apiMessage.getType() == ApiMessage.MessageType.CURVE_DATA) {

            CurveDataMessage curveDataMessage = (CurveDataMessage) apiMessage;

            Long id = curveDataMessage.getId();

            CurveItem item = CurveItem.fromAbstractDataItem(curveDataMessage.getData(),
                    metaDataCache.getMetaData(id).getIndexType() != LogIndexType.MEASURED_DEPTH);

            if (notOld(id, item.getKey())) {

                realTimeCache.add(id, item);
                eventBus.post(new RealtimeItem(id, item));

            } else {

                eventBus.post(new ReloadTask(id, item.getKey(), LocalDateTime.now().plusMinutes(5)));
            }
        } else {

            log.info("Some apiMessage: {}", apiMessage);
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

            realTimeCache.removeAll();

            //TODO handler stop event?
        }
    }

    private boolean notOld(Long id, Double key) {
        Double lastKey = metaDataCache.getMetaData(id).getLastDBKey();
        return lastKey == null || Double.compare(key, lastKey) > 0;
    }
}
