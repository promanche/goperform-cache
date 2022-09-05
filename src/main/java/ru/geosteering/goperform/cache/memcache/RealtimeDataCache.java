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

    public void merge(Long id, Collection<CurveItem> histItems) {

        if (histItems != null && !histItems.isEmpty()) {
            PriorityQueue<CurveItem> queue =
                    cache.computeIfAbsent(id, k -> new PriorityQueue<>(config.HISTORY_REQUEST_LIMIT, Comparator.comparing(CurveItem::getKey)));

            synchronized (queue) {

                int before = queue.size();
                log.info("Curve {}. Before merging caches: real_size={}, hist_size={}", id, before, histItems.size());

                queue.removeAll(histItems);
                int after = queue.size();
                queue.addAll(histItems);
                log.info("Found {} duplicates. After merging caches: real_size={}", before - after, queue.size());

                save(id, queue);
            }
        }
    }
}
