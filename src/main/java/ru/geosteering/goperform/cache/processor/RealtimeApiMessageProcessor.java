package ru.geosteering.goperform.cache.processor;

import io.nats.client.ConnectionListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.memcache.RealTimeCurveDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.EventListener;
import ru.geosteering.goperform.cache.model.event.*;
import ru.geosteering.goperform.cache.service.EventBus;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealtimeApiMessageProcessor implements EventListener {

    private final MetaDataCache metaDataCache;
    private final RealTimeCurveDataCache realTimeCache;

    @PostConstruct
    private void register() {
        EventBus.register(
                List.of(
                        Event.EventType.REALTIME_API_MESSAGE,
                        Event.EventType.CURVE_NOT_ACTIVE,
                        Event.EventType.NATS_CONNECTION_STATUS
                ),
                this);
    }

    @Override
    public void onEvent(Event<?> event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case REALTIME_API_MESSAGE:
                onApiMessage((RealTimeApiMessage) event);
                break;
            case CURVE_NOT_ACTIVE:
                onCurveNotActive((CurveNotActive) event);
                break;
            case NATS_CONNECTION_STATUS:
                onConnectionStatus((NatsConnectionStatus) event);
                break;
            default:
                break;
        }
    }

    private void onApiMessage(RealTimeApiMessage event) {

        ApiMessage apiMessage = event.getPayload();

        if (apiMessage.getType() == ApiMessage.MessageType.CURVE_DATA) {

            CurveDataMessage curveDataMessage = (CurveDataMessage) apiMessage;
            CurveItem item = CurveItem.fromAbstractDataItem(curveDataMessage.getData());
            Long curveId = curveDataMessage.getId();

            if (notOld(curveId, item.getKey())) {

                realTimeCache.add(curveId, item);
                EventBus.post(new NewCurveItem(curveId, item));

            } else {

                EventBus.post(new OldCurveItem(curveId, item));
            }
        } else {

            log.info("Some apiMessage: {}", apiMessage);
        }
    }

    private void onCurveNotActive(CurveNotActive event) {
        realTimeCache.clearById(event.getPayload());
    }

    private void onConnectionStatus(NatsConnectionStatus event) {

        ConnectionListener.Events status = event.getPayload();

        if (status == ConnectionListener.Events.DISCONNECTED || status == ConnectionListener.Events.CLOSED) {

            Executors.newSingleThreadScheduledExecutor()
                    .schedule(realTimeCache::clearAll, 2000, TimeUnit.MILLISECONDS);
        }
    }

    private boolean notOld(Long id, Double key) {
        Double lastKey = metaDataCache.getMetaData(id).getLastDBKey();
        return lastKey == null || key > lastKey;
    }
}
