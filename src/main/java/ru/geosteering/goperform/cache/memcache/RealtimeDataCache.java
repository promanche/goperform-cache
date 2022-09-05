package ru.geosteering.goperform.cache.memcache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.processor.EventDispatcher;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;

import java.util.*;

@Component
@Slf4j
public class RealtimeDataCache extends AbstractCurveItemCache {

    private final MetaDataCache metaDataCache;

    public RealtimeDataCache(MainRepository repository, Config config, MetaDataCache metaDataCache) {
        super(repository, config);
        this.metaDataCache = metaDataCache;
    }

    protected void save(Long id, PriorityQueue<CurveItem> items) {

        if (metaDataCache.isHistoryLoaded(id)) {

            while (items.size() >= config.BATCH_SIZE + config.MARGIN_SIZE) {

                ArrayList<CurveItem> itemsBatch = new ArrayList<>(config.BATCH_SIZE);
                for (int i = 0; i < config.BATCH_SIZE; i++) {
                    itemsBatch.add(items.poll());
                }

                repository.saveItems(List.of(ItemDto.fromItemsList(id, itemsBatch)));

                EventDispatcher.getInstance().onItemsBatch(id, itemsBatch);
            }
        }
    }

    public void merge(Long curveId, List<CurveItem> histItems) {

        PriorityQueue<CurveItem> items = cache.get(curveId);

        synchronized (items) {

            int before = items.size();
            log.info("Curve {}. Before merging caches: real_size={}, hist_size={}", curveId, before, histItems.size());

            items.removeAll(histItems);
            int after = items.size();
            items.addAll(histItems);
            log.info("Found {} duplicates. After merging caches: real_size={}", before - after, items.size());

            save(curveId, items);
        }
    }
}
