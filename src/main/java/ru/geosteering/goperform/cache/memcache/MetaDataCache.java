package ru.geosteering.goperform.cache.memcache;

import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveInfoMessage;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.model.event.*;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.service.EventBus;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class MetaDataCache {

    private final Config config;
    private final MainRepository repository;
    private final Map<Long, MetaData> metaDataMap = new ConcurrentHashMap<>();
    private final Map<Long, LocalDateTime> activeCurves = new ConcurrentHashMap<>();
    private final Set<Long> historyLoaded = ConcurrentHashMap.newKeySet();

    @PostConstruct
    private void loadFromDB() {
        repository.getAllMetaData()
                .forEach(metaData -> metaDataMap.put(metaData.getId(), metaData));

        log.info("MetaData loaded");
    }

    @Scheduled(fixedRate = 1, timeUnit = TimeUnit.MINUTES)
    private void checkCurveActivity() {

        synchronized (activeCurves) {

            LocalDateTime now = LocalDateTime.now();

            activeCurves.entrySet().removeIf(entry -> {

                boolean isNotActive = entry.getValue().isBefore(now.minusMinutes(1));

                if (isNotActive) {
                    log.info("Curve {} is not active", entry.getKey());
                    historyLoaded.remove(entry.getKey());
                    EventBus.post(new CurveNotActive(entry.getKey()));
                }

                return isNotActive;
            });
        }
    }

    public MetaData getMetaData(Long id) {
        return metaDataMap.computeIfAbsent(id, this::requestInfo);
    }

    public void addActiveCurve(Long id) {

        LocalDateTime previous = activeCurves.put(id, LocalDateTime.now());

        if (previous == null) {
            historyLoaded.remove(id);
            EventBus.post(new NewActiveCurve(id));
        }
    }

    public boolean isActive(Long id) {
        return activeCurves.containsKey(id);
    }

    public void addHistoryLoaded(Long id) {
        historyLoaded.add(id);
    }

    public boolean isHistoryLoaded(Long id) {
        return historyLoaded.contains(id);
    }


    private MetaData requestInfo(Long id) {

        MetaData metaData = null;

        CurveDataRequest request = new CurveDataRequest();
        request.setCurveId(id);
        request.setInfoOnly(true);
        request.setWithRange(true);

        Message response = NatsConnector.sendRequest(config.SUBJECT, StaticMapper.toBytes(request));

        if (response != null) {
            ApiMessage apiMessage = StaticMapper.parseObject(new String(response.getData()), ApiMessage.class);
            if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.CURVE_INFO) {
                metaData = new MetaData(((CurveInfoMessage) apiMessage).getCurveInfo());
            }
        }

        return metaData;
    }
}
