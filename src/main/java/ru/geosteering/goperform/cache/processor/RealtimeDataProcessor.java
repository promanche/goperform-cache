package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.memcache.RealtimeDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataProcessor implements DefaultEventProcessor {

    private final MetaDataCache metaDataCache;
    private final RealtimeDataCache realTimeCache;

    @Override
    public void onRealtimeApiMessage(ApiMessage apiMessage) {

        if (apiMessage.getType() == ApiMessage.MessageType.CURVE_DATA) {

            CurveDataMessage curveDataMessage = (CurveDataMessage) apiMessage;

            Long id = curveDataMessage.getId();

            MetaData metaData = metaDataCache.getMetaData(id);

            if (metaData != null) {

                CurveItem item = CurveItem.fromAbstractDataItem(curveDataMessage.getData(),
                        metaData.getIndexType() != LogIndexType.MEASURED_DEPTH);

                if (notOld(id, item.getKey())) {

                    realTimeCache.add(id, item);
                    EventDispatcher.getInstance().onRealtimeCurveItem(id, item);

                } else {

                    EventDispatcher.getInstance().onOldItem(id, item);
                }
            }

        } else {

            log.info("Some apiMessage: {}", apiMessage);
        }
    }

    @Override
    public void onReloadData(Long id, Double from) {
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
