package ru.geosteering.goperform.cache.service;

import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveInfoMessage;
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
import java.util.concurrent.ExecutionException;

@Service
@Slf4j
@RequiredArgsConstructor
public class MetaDataProcessor {

    private final MainRepository repository;
    private final Map<Long, MetaData> dataMap = new ConcurrentHashMap<>();

    @Setter
    private NatsConnector connector;

    @PostConstruct
    private void loadFromDB() {
        repository.getAllMetaData()
                .forEach(metaData -> dataMap.put(metaData.getId(), metaData));
    }

    public MetaData getMetaData(Long id) {
        return dataMap.computeIfAbsent(id, this::requestInfo);
    }

    public void refresh(Long id, CurveItem item) {

        MetaData metaData = getMetaData(id);

        synchronized (metaData) {
            LogDataType typeLogData = metaData.getTypeLogData();
            if (typeLogData == LogDataType.DOUBLE) {

                Double value = (Double) item.getValue();

                if (metaData.getMinValue() == null || metaData.getMinValue() > value) {
                    metaData.setMinValue(value);
                }

                if (metaData.getMaxValue() == null || metaData.getMaxValue() < value) {
                    metaData.setMaxValue(value);
                }
            }
        }
    }

    public LogIndexType getIndexType(Long id) {
        return getMetaData(id).getIndexType();
    }

    public LogDataType getDataType(Long id) {
        return getMetaData(id).getTypeLogData();
    }

    private MetaData requestInfo(Long id) {
        MetaData metaData = null;

        CurveDataRequest request = new CurveDataRequest();
        request.setCurveId(id);
        request.setInfoOnly(true);
        request.setWithRange(true);

        log.info("Curve info request: {}", request);

        Message response = null;
        try {
            response = connector.sendRequest(MapperUtils.toBytes(request));
        } catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        if (response != null) {
            ApiMessage apiMessage = MapperUtils.parseApiMessage(new String(response.getData()), response.getSubject());
            if (apiMessage != null && apiMessage.getType() == ApiMessage.MessageType.CURVE_INFO) {
                metaData = new MetaData(((CurveInfoMessage) apiMessage).getCurveInfo());
            }
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
}
