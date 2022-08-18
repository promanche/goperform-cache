package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.model.event.*;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.service.EventBus;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class MetaDataProcessorAlt implements EventListener {

    private final MetaDataCache metaDataCache;
    private final MainRepository repository;

    @PostConstruct
    private void register() {
        EventBus.register(
                List.of(
                        Event.EventType.NEW_CURVE_ITEM,
                        Event.EventType.ITEMS_BATCH_COLLECTED
                ),
                this);
    }

    @Override
    public void onEvent(Event<?> event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case NEW_CURVE_ITEM:
                onNewCurveItem((NewCurveItem) event);
                break;
            case ITEMS_BATCH_COLLECTED:
                onItemsBatchCollected((ItemsBatchCollected) event);
                break;
            default:
                break;
        }
    }

    private void onNewCurveItem(NewCurveItem event) {

        CurveItem item = event.getPayload();

        MetaData metaData = metaDataCache.getMetaData(event.getCurveId());

        synchronized (metaData) {

            LogDataType typeLogData = metaData.getTypeLogData();

            if (typeLogData == LogDataType.DOUBLE || typeLogData == LogDataType.LONG) {
                Double value = (Double) item.getValue();

                if (metaData.getMinValue() == null || metaData.getMinValue() > value) {
                    metaData.setMinValue(value);
                }

                if (metaData.getMaxValue() == null || metaData.getMaxValue() < value) {
                    metaData.setMaxValue(value);
                }
            }
        }

        metaDataCache.addActiveCurve(event.getCurveId());
    }

    private void onItemsBatchCollected(ItemsBatchCollected event) {

        MetaData metaData = metaDataCache.getMetaData(event.getCurveId());

        synchronized (metaData) {

            List<CurveItem> items = event.getPayload();
            Double firstKey = items.get(0).getKey();
            Double lastKey = items.get(items.size() - 1).getKey();

            if (metaData.getLastDBKey() == null || metaData.getLastDBKey() < lastKey) {
                metaData.setLastDBKey(lastKey);
            }

            if (metaData.getFirstDBKey() == null || metaData.getFirstDBKey() > firstKey) {
                metaData.setFirstDBKey(firstKey);
            }

            metaData.setItemsInDB(metaData.getItemsInDB() + items.size());

            repository.saveOrUpdateMetaData(metaData);
        }
    }
}
