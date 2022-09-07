package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
@RequiredArgsConstructor
public class MetaDataProcessor implements DefaultEventProcessor {

    private final MetaDataCache metaDataCache;
    private final MainRepository repository;
    private final AtomicInteger counter = new AtomicInteger();
    private long timer = System.nanoTime();

    @Override
    public void onRealtimeCurveItem(Long id, CurveItem item) {
        counter.incrementAndGet();
        metaDataCache.addActiveCurve(id);
        updateByNewItem(id, item);
    }

    @Override
    public void onHistoryCurveItem(Long id, CurveItem item) {
        counter.incrementAndGet();
        updateByNewItem(id, item);
    }

    @Override
    public void onItemsBatch(Long id, List<CurveItem> items) {

        MetaData metaData = metaDataCache.getMetaData(id);

        synchronized (metaData) {

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

    @Override
    public void onReloadData(Long id, Double from) {
        metaDataCache.removeFromLoaded(id);
        metaDataCache.reloadById(id);
    }

    @Override
    public void onLoadResult(Long id, LoadResult result, Double from, Double to) {
        if (result == LoadResult.DONE) {
            metaDataCache.addHistoryLoaded(id);
        }
    }

    private void updateByNewItem(Long id, CurveItem item) {

        MetaData metaData = metaDataCache.getMetaData(id);

        synchronized (metaData) {

            Double key = item.getKey();

            if (metaData.getMinKey() == null || Double.compare(metaData.getMinKey(), key) > 0) {
                metaData.setMinKey(key);
            }

            if (metaData.getMaxKey() == null || Double.compare(metaData.getMaxKey(), key) < 0) {
                metaData.setMaxKey(key);
                metaData.setLastValue(item.getValue());
            }

            if (metaData.getAxisDefinition() != null) {
                return;
            }

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
    }


    @Scheduled(fixedDelay = 60, timeUnit = TimeUnit.SECONDS)
    private void heartbeat() {

        long seconds = (System.nanoTime() - timer) / 1_000_000_000;
        timer = System.nanoTime();
        int received = counter.getAndSet(0);

        log.info("Curves info: active {}, history loaded {}, received {} points in {} sec",
                metaDataCache.getActiveCount(), metaDataCache.getLoadedCount(), received, seconds);
    }
}
