package ru.geosteering.goperform.cache.processor;

import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.CurveItem;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
public class EventDispatcher implements EventProcessor {

    private static EventDispatcher eventDispatcher;

    private final Map<Class<? extends DefaultEventProcessor>, DefaultEventProcessor> processorMap = new HashMap<>();

    public EventDispatcher(List<DefaultEventProcessor> processors) {
        processors.forEach(processor -> processorMap.put(processor.getClass(), processor));
    }

    public static EventDispatcher getInstance() {
        return eventDispatcher;
    }

    @PostConstruct
    private void initDispatcher() {
        eventDispatcher = this;
    }

    @Override
    public void onRealtimeApiMessage(ApiMessage apiMessage) {

        processorMap.get(RealtimeDataProcessor.class).onRealtimeApiMessage(apiMessage);

    }

    @Override
    public void onHistoryApiMessage(ApiMessage apiMessage, String subject) {

        processorMap.get(HistoryDataProcessor.class).onHistoryApiMessage(apiMessage, subject);

    }

    @Override
    public void onRealtimeCurveItem(Long id, CurveItem item) {

        processorMap.get(MetaDataProcessor.class).onRealtimeCurveItem(id, item);
        processorMap.get(CurveDataLoadProcessor.class).onRealtimeCurveItem(id, item);
        processorMap.get(WebSocketMessageProcessor.class).onRealtimeCurveItem(id, item);

    }

    @Override
    public void onHistoryCurveItem(Long id, CurveItem item) {

        processorMap.get(MetaDataProcessor.class).onHistoryCurveItem(id, item);

    }

    @Override
    public void onItemsBatch(Long id, List<CurveItem> items) {

        processorMap.get(MetaDataProcessor.class).onItemsBatch(id, items);
        processorMap.get(CurveSegmentProcessor.class).onItemsBatch(id, items);

    }

    @Override
    public void onOldItem(Long id, CurveItem item) {

        processorMap.get(CurveDataLoadProcessor.class).onOldItem(id, item);

    }

    @Override
    public void onReloadData(Long id, Double from) {

        processorMap.get(HistoryDataProcessor.class).onReloadData(id, from);
        processorMap.get(RealtimeDataProcessor.class).onReloadData(id, from);
        processorMap.get(MetaDataProcessor.class).onReloadData(id, from);
        processorMap.get(CurveSegmentProcessor.class).onReloadData(id, from);

    }

    @Override
    public void onConnect() {

        processorMap.get(CurveDataLoadProcessor.class).onConnect();

    }

    @Override
    public void onDisconnect() {

        processorMap.get(CurveDataLoadProcessor.class).onDisconnect();
        processorMap.get(HistoryDataProcessor.class).onDisconnect();
        processorMap.get(RealtimeDataProcessor.class).onDisconnect();

    }

    @Override
    public void onLoadResult(Long id, LoadResult result, Double from, Double to) {

        processorMap.get(CurveDataLoadProcessor.class).onLoadResult(id, result, from, to);
        processorMap.get(MetaDataProcessor.class).onLoadResult(id, result, from, to);
        processorMap.get(WebSocketMessageProcessor.class).onLoadResult(id, result, from, to);

    }
}
