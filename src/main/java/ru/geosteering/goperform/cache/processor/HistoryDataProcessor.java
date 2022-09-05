package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.memcache.HistoryDataCache;
import ru.geosteering.goperform.cache.memcache.MetaDataCache;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class HistoryDataProcessor implements DefaultEventProcessor {

    private final HistoryDataCache historyCache;
    private final MetaDataCache metaDataCache;
    private final Config config;

    private final Map<Long, AtomicInteger> receivedCount = new ConcurrentHashMap<>();
    private final Map<Long, Set<CurveItem>> buffer = new ConcurrentHashMap<>();

    @Override
    public void onHistoryApiMessage(ApiMessage apiMessage, String subject) {

        ApiMessage.MessageType type = apiMessage.getType();

        switch (type) {
            case CURVE_DATA:
                processCurveData((CurveDataMessage) apiMessage);
                break;
            case DATA_END:
                processDataEnd((DataEndMessage) apiMessage, subject);
                break;
            default:
                log.info("Some apiMessage: {}", apiMessage);
                break;
        }
    }

    @Override
    public void onReloadData(Long id, Double from) {
        historyCache.remove(id);
    }

    @Override
    public void onDisconnect() {
        receivedCount.clear();
        buffer.clear();
        historyCache.removeAll();
    }

    private void processCurveData(CurveDataMessage curveDataMessage) {

        Long id = curveDataMessage.getId();

        CurveItem item = CurveItem.fromAbstractDataItem(curveDataMessage.getData(),
                metaDataCache.getMetaData(id).getIndexType() != LogIndexType.MEASURED_DEPTH);

        buffer.computeIfAbsent(id, key -> ConcurrentHashMap.newKeySet(config.HISTORY_REQUEST_LIMIT))
                .add(item);

        receivedCount.computeIfAbsent(id, key -> new AtomicInteger(0))
                .incrementAndGet();

        EventDispatcher.getInstance().onHistoryCurveItem(id, item);
    }

    private void processDataEnd(DataEndMessage dataEndMessage, String subject) {

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        Long id = parseId(subject);

        int sent = dataEndMessage.getSentCount();
        int received = receivedCount.containsKey(id) ? receivedCount.remove(id).get() : -1;

        if (sent == 0) {
            log.info("Curve {} data loaded, {}", id, dataEndMessage);
            EventDispatcher.getInstance().onLoadResult(id, LoadResult.DONE);

        } else if (sent != received) {
            log.error("Received count {} not equals to sent {}", received, sent);
            EventDispatcher.getInstance().onLoadResult(id, LoadResult.ERROR);

        } else {
            log.info("Curve {} history part received, {}", id, dataEndMessage);
            drainToCache(id);
            EventDispatcher.getInstance().onLoadResult(id, LoadResult.PART);
        }

        buffer.remove(id);
    }

    private Long parseId(String subject) {

        try {
            String[] arr = subject.split("\\.");
            return Long.parseLong(arr[arr.length - 1]);
        } catch (Exception e) {
            log.error("Parsing id from subject exception: {}", subject, e);
            return null;
        }
    }

    private void drainToCache(Long id) {
        log.info("Drain buffer to cache. Curve: {}, items: {}", id, buffer.get(id).size());
        historyCache.addAll(id, buffer.get(id));
    }
}
