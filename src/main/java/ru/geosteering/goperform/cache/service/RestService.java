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
    private final DataReloader reloader;

    public List<CacheItem> getDataItems(Long id, Double from, Double to, Integer limit) {

        if (storage.isHistoryLoaded(id)) {
            log.debug("Begin response preparing for id {}", id);

            List<String> caches;

            if (from == null && to == null) {
                caches = repository.getAllCaches(id);
            } else {
                from = from == null ? Double.MIN_VALUE : from;
                to = to == null ? Double.MAX_VALUE : to;
                caches = repository.getFromTo(id, from, to);
            }

            log.debug("Caches from db loaded: {} records", caches.size());

            List<List<CacheItem>> fromDB = caches.stream()
                    .map(CacheUtils::parseCacheItems)
                    .collect(Collectors.toList());

            List<CacheItem> fromStorage = storage.getFromStorage(id, from, to);

            if (!fromDB.isEmpty()) {
                if (from != null) {
                    Double finalFrom = from;
                    fromDB.get(0).removeIf(i -> i.getKey() < finalFrom);
                }
                if (to != null) {
                    Double finalTo = to;
                    fromDB.get(fromDB.size() - 1).removeIf(i -> i.getKey() > finalTo);
                }
            }

            List<CacheItem> result = new ArrayList<>(fromDB.size() * 1000 + fromStorage.size());
            fromDB.forEach(result::addAll);
            result.addAll(fromStorage);

            if (limit != null && limit > 2) {
                double factor = 1;
                while (result.size() > limit && factor > 0.01) {
                    factor -= 0.01;
                    int before = result.size();
                    result = CacheUtils.approximate(result, factor);
                    log.debug("Approximate step for id {}, limit{}, factor {}, before {}, after {}", id, limit, factor, before, result.size());
                }
            }

            log.info("Response for id {} prepared. Items count: {}", id, result.size());

            return result;
        }

        loader.loadByRequest(id);

        return null;
    }

    public ApiMessage getCurveInfo(Long id) {

        CurveDataRequest request = new CurveDataRequest();
        request.setCurveId(id);
        request.setInfoOnly(true);
        request.setWithRange(true);

        log.info("Curve info request: {}", request);

        Message response = null;
        try {
            response = connector.sendRequest(CacheUtils.toBytes(request));
        } catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        return response == null ? null : CacheUtils.parseApiMessage(new String(response.getData()), response.getSubject());
    }

    public void reloadCurve(Long id, Double from) {
        reloader.addForReload(id, from, 2);
    }
}
