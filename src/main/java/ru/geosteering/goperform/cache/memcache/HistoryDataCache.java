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
public class HistoryDataCache extends AbstractCurveItemCache {

    public HistoryDataCache(MainRepository repository, Config config, EventDispatcher eventDispatcher) {
        super(repository, config, eventDispatcher);
    }

    protected void save(Long id, PriorityQueue<CurveItem> items) {

        List<ItemDto> transfer = new ArrayList<>();

        while (items.size() >= config.BATCH_SIZE) {

            ArrayList<CurveItem> itemsBatch = new ArrayList<>(config.BATCH_SIZE);

            for (int i = 0; i < config.BATCH_SIZE; i++) {
                itemsBatch.add(items.poll());
            }

            transfer.add(ItemDto.fromItemsList(id, itemsBatch));

            eventDispatcher.onItemsBatch(id, itemsBatch);
        }

        repository.saveItems(transfer);
    }

    public List<CurveItem> drain(Long id) {

        PriorityQueue<CurveItem> items = cache.remove(id);

        List<CurveItem> result = new ArrayList<>(items.size());

        while (!items.isEmpty()) {
            result.add(items.poll());
        }

        return result;
    }
}
