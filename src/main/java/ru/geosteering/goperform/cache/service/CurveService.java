package ru.geosteering.goperform.cache.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.memcache.*;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.model.event.task.LoadITask;
import ru.geosteering.goperform.cache.model.event.task.ReloadTask;
import ru.geosteering.goperform.cache.processor.CurveSegmentProcessor;
import ru.geosteering.goperform.cache.processor.EventBus;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class CurveService {

    private final MainRepository repository;
    private final MetaDataCache metaDataCache;
    private final HistoryDataCache historyDataCache;
    private final RealtimeDataCache realtimeDataCache;
    private final CurveSegmentProcessor segmentProcessor;
    private final EventBus eventBus;

    public Object getCurveData(Long id, Double from, Double to, Integer scale) {

        if (metaDataCache.isHistoryLoaded(id)) {

            log.debug("Begin response preparing for id {}", id);

            if (from != null || to != null) {
                from = from == null ? Double.MIN_VALUE : from;
                to = to == null ? Double.MAX_VALUE : to;
            }

            if (scale != null && metaDataCache.getMetaData(id).getScaleSet().contains(scale)) {

                List<CurveSegment> result = repository.getSegmentsFromTo(id, scale, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveSegment>>) str -> StaticMapper.parseListOf(str, CurveSegment.class).stream())
                        .collect(Collectors.toList());

                List<CurveItem> fromCache = historyDataCache.get(id, from, to);
                fromCache.addAll(realtimeDataCache.get(id, from, to));

                result.addAll(segmentProcessor.getAndCompleteSegmentsFromMemory(id, scale, from, to, fromCache));

                log.info("Response for id {} prepared. Result list size: {}", id, result.size());

                return result;

            } else {

                List<CurveItem> result = repository.getItemsFromTo(id, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                        .collect(Collectors.toList());

                result.addAll(historyDataCache.get(id, from, to));
                result.addAll(realtimeDataCache.get(id, from, to));

                log.info("Response for id {} prepared. Result list size: {}", id, result.size());

                return result;
            }
        }

        log.debug("Curve data id {} not yet loaded", id);
        eventBus.post(new LoadITask(id));

        return null;
    }

    public MetaData getCurveInfo(Long id) {
        return metaDataCache.getMetaData(id);
    }

    public void reloadCurve(Long id, Double from) {
        eventBus.post(new ReloadTask(id, from, LocalDateTime.now().plusMinutes(1)));
    }
}
