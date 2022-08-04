package ru.geosteering.goperform.cache.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.model.*;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.MapperUtils;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@Slf4j
@AllArgsConstructor
public class RestService {

    private final Storage storage;
    private final MainRepository repository;
    private final HistoryLoader loader;
    private final DataReloader reloader;
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

                List<CurveSegment> result = repository.getLinesFromTo(id, scale, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveSegment>>) s -> MapperUtils.parseCacheLines(s).stream())
                        .collect(Collectors.toList());

                result.addAll(approximator.getLines(id, scale, from, to));

                addItemsFromStorage(result, id, from, to, scale);

                log.info("Response for id {} prepared. Result list size: {}", id, result.size());

                return result;

            } else {

                List<CurveItem> result = repository.getItemsFromTo(id, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveItem>>) s -> MapperUtils.parseCacheItems(s).stream())
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
        loader.applyStatus(id, LoadStatus.BLOCKED);
        reloader.addForReload(id, from, 2);
    }

    private void addItemsFromStorage(List<CurveSegment> segments, Long id, Double from, Double to, int scale) {

        List<CurveItem> fromStorage = storage.getFromStorage(id, from, to);

        if (!fromStorage.isEmpty()) {

            int secondsOnPixel = scale * 60 / 120;

            CurveSegment last = segments.isEmpty() ? null : segments.get(segments.size() - 1);

            for (CurveItem item : fromStorage) {

                if (last != null && item.getKey() - last.getFirstKey() < (secondsOnPixel - 1) * 1000) {
                    Double value = (Double) item.getValue();
                    if (value > last.getMaxVal()) {
                        last.setMaxVal(value);
                    }
                    if (value < last.getMinVal()) {
                        last.setMinVal(value);
                    }
                    last.setLastKey(item.getKey());

                } else {
                    CurveSegment segment = new CurveSegment();
                    segment.setFirstKey(item.getKey());
                    segment.setLastKey(item.getKey());
                    segment.setMinVal((Double) item.getValue());
                    segment.setMaxVal((Double) item.getValue());
                    segments.add(segment);

                    last = segment;
                }
            }
        }
    }
}
