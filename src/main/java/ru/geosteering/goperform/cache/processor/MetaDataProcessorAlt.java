package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.model.event.Event;
import ru.geosteering.goperform.cache.model.event.item.*;
import ru.geosteering.goperform.cache.model.event.task.ClearTask;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class MetaDataProcessorAlt implements EventProcessor {

    private final MetaDataCache metaDataCache;
    private final MainRepository repository;
    private final EventBus eventBus;

    @PostConstruct
    private void register() {
        eventBus.register(
                List.of(
                        Event.EventType.REALTIME_ITEM,
                        Event.EventType.HISTORY_ITEM,
                        Event.EventType.ITEMS_BATCH,
                        Event.EventType.CLEAR_TASK
                ),
                this);
    }

    @Override
    public void processEvent(Event event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case REALTIME_ITEM:
                RealtimeItem real = (RealtimeItem) event;
                updateByNewItem(real.getId(), real.getCurveItem(), true);
                break;
            case HISTORY_ITEM:
                HistoryItem hist = (HistoryItem) event;
                updateByNewItem(hist.getId(), hist.getCurveItem(), false);
                break;
            case ITEMS_BATCH:
                onItemsBatchCollected((ItemsBatch) event);
                break;
            case CLEAR_TASK:
                metaDataCache.reloadById(((ClearTask) event).getId());
                break;
            default:
                break;
        }
    }

    private void updateByNewItem(Long id, CurveItem item, boolean isReal) {

        MetaData metaData = metaDataCache.getMetaData(id);

        if (metaData.getAxisDefinition() != null) {
            return;
        }

        synchronized (metaData) {

            LogDataType typeLogData = metaData.getTypeLogData();

            if (typeLogData == LogDataType.DOUBLE || typeLogData == LogDataType.LONG) {
                Double value = (Double) item.getValue();

                if (metaData.getMinValue() == null || Double.compare(metaData.getMinValue(), value) > 0) {
                    metaData.setMinValue(value);
                }

                if (metaData.getMaxValue() == null || Double.compare(metaData.getMaxValue(), value) < 0) {
                    metaData.setMaxValue(value);
                }
            }
        }

        if (isReal) {
            metaDataCache.addActiveCurve(id);
        }
    }

    private void onItemsBatchCollected(ItemsBatch event) {

        MetaData metaData = metaDataCache.getMetaData(event.getId());

        synchronized (metaData) {

            List<CurveItem> items = event.getItems();
            Double firstKey = items.get(0).getKey();
            Double lastKey = items.get(items.size() - 1).getKey();

            if (metaData.getLastDBKey() == null || Double.compare(metaData.getLastDBKey(), lastKey) < 0) {
                metaData.setLastDBKey(lastKey);
            }

            if (metaData.getFirstDBKey() == null || Double.compare(metaData.getFirstDBKey(), firstKey) > 0) {
                metaData.setFirstDBKey(firstKey);
            }

            metaData.setItemsInDB(metaData.getItemsInDB() + items.size());

            repository.saveOrUpdateMetaData(metaData);
        }
    }
}
