package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.CacheItem;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.CacheRepository;
import ru.geosteering.goperform.cache.storage.Storage;
import ru.geosteering.goperform.cache.utils.CacheUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
@AllArgsConstructor
public class RestService {

    private final NatsConnector connector;
    private final Storage storage;
    private final CacheRepository repository;
    private final HistoryLoader loader;

    public List<CacheItem> getDataItems(Long id, Double from, Double to, boolean hasFromTo, Integer limit) {

        if (storage.isHistoryLoaded(id)) {
            log.info("Begin response preparing for id {}", id);

            List<CacheItem> result = new ArrayList<>();

            List<String> caches = hasFromTo ? repository.getFromTo(id, from, to) : repository.getAll(id);

            List<List<CacheItem>> fromDB = caches.stream()
                    .map(CacheUtils::parseCacheItems)
                    .collect(Collectors.toList());

            if (!fromDB.isEmpty()) {
                fromDB.get(0).removeIf(i -> i.getKey() < from);
                fromDB.get(fromDB.size() - 1).removeIf(i -> i.getKey() > to);
                fromDB.forEach(result::addAll);
            }

            result.addAll(storage.getFromStorage(id, from, to));

            if (limit != null && limit > 2) {
                double factor = 1;
                log.info("Approximating start: limit {}, size {}", limit, result.size());
                while (result.size() > limit && factor > 0.01) {
                    factor -= 0.01;
                    int before = result.size();
                    result = CacheUtils.approximate(result, factor);
                    log.info("Approximate step: factor {}, before: {}, after: {}", factor, before, result.size());
                }
            }

            log.info("Response for id {} prepared. Items count: {}", id, result.size());
            return result;
        }

        loader.loadCurve(id);

        return null;
    }

    public ApiMessage getCurveInfo(Long id) {

        CurveDataRequest request = new CurveDataRequest();
        request.setCurveId(id);
        request.setInfoOnly(true);

        Message response = null;
        try {
            response = connector.sendRequest(CacheUtils.toBytes(request));
        } catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        return response == null ? null : CacheUtils.parseApiMessage(new String(response.getData()), response.getSubject());
    }
}
