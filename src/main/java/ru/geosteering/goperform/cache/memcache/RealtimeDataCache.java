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

    public RealtimeDataCache(MainRepository repository, Config config, EventDispatcher eventDispatcher, MetaDataCache metaDataCache) {
        super(repository, config, eventDispatcher);
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

                eventDispatcher.onItemsBatch(id, itemsBatch);
            }
        }
    }

    public void merge(Long curveId, List<CurveItem> histItems) {

        PriorityQueue<CurveItem> items = cache.get(curveId);

        synchronized (items) {

            int before = items.size();
            log.info("Curve {}. Before merging caches: real_size={}, hist_size={}", curveId, items.size(), before);

            items.removeAll(histItems);

            log.info("Curve {}. Found {} duplicates. After merging caches: real_size={}", curveId, before - items.size(), items.size());

            items.addAll(histItems);

            save(curveId, items);
        }
    }
}
