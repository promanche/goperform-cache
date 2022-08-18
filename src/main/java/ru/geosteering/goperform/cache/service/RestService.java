package ru.geosteering.goperform.cache.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class RestService {

    private final Storage storage;
    private final MainRepository repository;
    private final DataLoader loader;
    private final MetaDataProcessor metaDataProcessor;
    private final Approximator approximator;

    public Object getCurveData(Long id, Double from, Double to, Integer scale) {

        if (storage.isHistoryLoaded(id)) {

            log.debug("Begin response preparing for id {}", id);

            if (from != null || to != null) {
                from = from == null ? Double.MIN_VALUE : from;
                to = to == null ? Double.MAX_VALUE : to;
            }

            if (scale != null && metaDataProcessor.getMetaData(id).getScaleSet().contains(scale)) {

                List<CurveSegment> result = repository.getSegmentsFromTo(id, scale, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveSegment>>) str -> StaticMapper.parseListOf(str, CurveSegment.class).stream())
                        .collect(Collectors.toList());

                List<CurveItem> fromStorage = storage.getFromStorage(id, from, to);

                result.addAll(approximator.getAndCompleteSegmentsFromMemory(id, scale, from, to, fromStorage));

                log.info("Response for id {} prepared. Result list size: {}", id, result.size());

                return result;

            } else {

                List<CurveItem> result = repository.getItemsFromTo(id, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveItem>>) str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                        .collect(Collectors.toList());

                result.addAll(storage.getFromStorage(id, from, to));

                log.info("Response for id {} prepared. Result list size: {}", id, result.size());

                return result;
            }
        }

        log.debug("Curve data id {} not yet loaded", id);
        loader.loadByRequest(id);

        return null;
    }

    public MetaData getCurveInfo(Long id) {
        return metaDataProcessor.getMetaData(id);
    }

    public void reloadCurve(Long id, Double from) {
        loader.addForReload(id, from, 10);
    }
}
