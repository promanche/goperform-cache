package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveInfoMessage;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.MapperUtils;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogDataType;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.v131.LogIndexType;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class MetaDataProcessor {

    private final MainRepository repository;
    private final Config config;
    private final Map<Long, MetaData> metaDataMap = new ConcurrentHashMap<>();

    @PostConstruct
    private void loadFromDB() {
        repository.getAllMetaData()
                .forEach(metaData -> metaDataMap.put(metaData.getId(), metaData));

        log.info("MetaData loaded");
    }

    public MetaData getMetaData(Long id) {
        return metaDataMap.computeIfAbsent(id, k -> requestInfo(k, false));
    }

    public void updateByNewItem(Long id, CurveItem item) {

        MetaData metaData = getMetaData(id);

        if (metaData.getAxisDefinition() != null) {
            return;
        }

        synchronized (metaData) {

            LogDataType typeLogData = metaData.getTypeLogData();

            if (typeLogData == LogDataType.DOUBLE || typeLogData == LogDataType.LONG) {

                Double value = (Double) item.getValue();
                Double key = item.getKey();

                if (metaData.getMinValue() == null || metaData.getMinValue() > value) {
                    metaData.setMinValue(value);
                }

                if (metaData.getMaxValue() == null || metaData.getMaxValue() < value) {
                    metaData.setMaxValue(value);
                }

                if (metaData.getMinKey() == null || metaData.getMinKey() > key) {
                    metaData.setMinKey(key);
                }

                if (metaData.getMaxKey() == null || metaData.getMaxKey() < key) {
                    metaData.setMaxKey(key);
                }
            }
        }
    }

    public LogIndexType getIndexType(Long id) {
        return getMetaData(id).getIndexType();
    }

    private MetaData requestInfo(Long id, boolean withRange) {

        MetaData metaData = null;

        CurveDataRequest request = new CurveDataRequest();
        request.setCurveId(id);
        request.setInfoOnly(true);
        request.setWithRange(withRange);

        Message response = NatsConnector.sendRequest(config.SUBJECT, MapperUtils.toBytes(request));

        if (response != null) {
            ApiMessage apiMessage = MapperUtils.parseObject(new String(response.getData()), ApiMessage.class);
            if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.CURVE_INFO) {
                metaData = new MetaData(((CurveInfoMessage) apiMessage).getCurveInfo());
            }
        }

        if (metaData != null) {
            repository.saveOrUpdateMetaData(metaData);
        }

        return metaData;
    }

    public void updateByItemsBatch(Long id, Double firstKey, Double lastKey, int count) {

        MetaData metaData = getMetaData(id);

        synchronized (metaData) {

            if (metaData.getLastDBKey() == null || metaData.getLastDBKey() < lastKey) {
                metaData.setLastDBKey(lastKey);
            }
            if (metaData.getFirstDBKey() == null || metaData.getFirstDBKey() > firstKey) {
                metaData.setFirstDBKey(firstKey);
            }
            metaData.setItemsInDB(metaData.getItemsInDB() + count);

            repository.saveOrUpdateMetaData(metaData);
        }
    }

    public void reloadById(Long id) {

        MetaData metaData = requestInfo(id, true);

        metaData.setFirstDBKey(repository.getFirstItemKey(id));
        metaData.setLastDBKey(repository.getLastItemKey(id));
        metaData.setItemsInDB(repository.getItemsRecords(id) * config.BATCH_SIZE);
        metaData.setScaleSet(repository.getSegmentsScales(id));

        metaDataMap.compute(id, (k, v) -> metaData);

        repository.saveOrUpdateMetaData(metaData);
    }
}
