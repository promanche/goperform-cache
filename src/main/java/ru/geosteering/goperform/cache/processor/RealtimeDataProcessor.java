package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.memcache.RealtimeDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataProcessor implements DefaultEventProcessor {

    private final MetaDataCache metaDataCache;
    private final RealtimeDataCache realTimeCache;

    private EventDispatcher eventDispatcher;

    @Override
    public void setEventDispatcher(EventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public void onRealtimeApiMessage(ApiMessage apiMessage) {

        if (apiMessage.getType() == ApiMessage.MessageType.CURVE_DATA) {

            CurveDataMessage curveDataMessage = (CurveDataMessage) apiMessage;

            Long id = curveDataMessage.getId();

            CurveItem item = CurveItem.fromAbstractDataItem(curveDataMessage.getData(),
                    metaDataCache.getMetaData(id).getIndexType() != LogIndexType.MEASURED_DEPTH);

            if (notOld(id, item.getKey())) {

                realTimeCache.add(id, item);
                eventDispatcher.onRealtimeCurveItem(id, item);

            } else {

                eventDispatcher.onOldItem(id, item);
            }
        } else {

            log.info("Some apiMessage: {}", apiMessage);
        }
    }

    @Override
    public void onReloadData(Long id) {
        realTimeCache.remove(id);
    }

    @Override
    public void onDisconnect() {
        realTimeCache.removeAll();
    }

    private boolean notOld(Long id, Double key) {
        Double lastKey = metaDataCache.getMetaData(id).getLastDBKey();
        return lastKey == null || Double.compare(key, lastKey) > 0;
    }
}
