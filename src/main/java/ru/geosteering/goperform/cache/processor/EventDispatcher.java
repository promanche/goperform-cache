package ru.geosteering.goperform.cache.processor;

import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.CurveItem;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
public class EventDispatcher implements EventProcessor {

    private final Map<Event, List<Class<? extends DefaultEventProcessor>>> eventMap = new HashMap<>();
    private final Map<Class<? extends DefaultEventProcessor>, DefaultEventProcessor> processorMap = new HashMap<>();

    public EventDispatcher(List<DefaultEventProcessor> processors) {
        processors.forEach(processor -> {
            processorMap.put(processor.getClass(), processor);
            processor.setEventDispatcher(this);
        });
    }

    // Порядок добавления классов в списки -> порядок обработки событий. Это важно!
    @PostConstruct
    private void initEventMap() {

        eventMap.put(Event.REALTIME_API_MESSAGE,
                List.of(
                        RealtimeDataProcessor.class
                ));

        eventMap.put(Event.HISTORY_API_MESSAGE,
                List.of(
                        HistoryDataProcessor.class
                ));

        eventMap.put(Event.REALTIME_CURVE_ITEM,
                List.of(
                        MetaDataProcessor.class,
                        CurveDataLoadProcessor.class,
                        WebSocketMessageProcessor.class
                ));

        eventMap.put(Event.HISTORY_CURVE_ITEM,
                List.of(
                        MetaDataProcessor.class
                ));

        eventMap.put(Event.ITEMS_BATCH,
                List.of(
                        MetaDataProcessor.class,
                        CurveSegmentProcessor.class,
                        WebSocketMessageProcessor.class
                ));

        eventMap.put(Event.OLD_ITEM,
                List.of(
                        CurveDataLoadProcessor.class
                ));

        eventMap.put(Event.RELOAD_DATA,
                List.of(
                        MetaDataProcessor.class,
                        HistoryDataProcessor.class,
                        RealtimeDataProcessor.class,
                        CurveSegmentProcessor.class
                ));

        eventMap.put(Event.CONNECTED,
                List.of(
                        CurveDataLoadProcessor.class
                ));

        eventMap.put(Event.DISCONNECTED,
                List.of(
                        CurveDataLoadProcessor.class,
                        HistoryDataProcessor.class,
                        RealtimeDataProcessor.class
                ));

        eventMap.put(Event.LOAD_RESULT,
                List.of(
                        CurveDataLoadProcessor.class,
                        WebSocketMessageProcessor.class
                ));
    }

    @Override
    public void onRealtimeApiMessage(ApiMessage apiMessage) {
        eventMap.get(Event.REALTIME_API_MESSAGE)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onRealtimeApiMessage(apiMessage));
    }

    @Override
    public void onHistoryApiMessage(ApiMessage apiMessage, String subject) {
        eventMap.get(Event.HISTORY_API_MESSAGE)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onHistoryApiMessage(apiMessage, subject));
    }

    @Override
    public void onRealtimeCurveItem(Long id, CurveItem item) {
        eventMap.get(Event.REALTIME_CURVE_ITEM)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onRealtimeCurveItem(id, item));
    }

    @Override
    public void onHistoryCurveItem(Long id, CurveItem item) {
        eventMap.get(Event.HISTORY_CURVE_ITEM)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onHistoryCurveItem(id, item));
    }

    @Override
    public void onItemsBatch(Long id, List<CurveItem> items) {
        eventMap.get(Event.ITEMS_BATCH)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onItemsBatch(id, items));
    }

    @Override
    public void onOldItem(Long id, CurveItem item) {
        eventMap.get(Event.OLD_ITEM)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onOldItem(id, item));
    }

    @Override
    public void onReloadData(Long id) {
        eventMap.get(Event.RELOAD_DATA)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onReloadData(id));
    }

    @Override
    public void onConnect() {
        eventMap.get(Event.CONNECTED)
                .stream()
                .map(processorMap::get)
                .forEach(DefaultEventProcessor::onConnect);
    }

    @Override
    public void onDisconnect() {
        eventMap.get(Event.DISCONNECTED)
                .stream()
                .map(processorMap::get)
                .forEach(DefaultEventProcessor::onDisconnect);
    }

    @Override
    public void onLoadResult(Long id, LoadResult result) {
        eventMap.get(Event.LOAD_RESULT)
                .stream()
                .map(processorMap::get)
                .forEach(pr -> pr.onLoadResult(id, result));
    }

    private enum Event {
        REALTIME_API_MESSAGE,
        HISTORY_API_MESSAGE,
        REALTIME_CURVE_ITEM,
        HISTORY_CURVE_ITEM,
        ITEMS_BATCH,
        OLD_ITEM,
        RELOAD_DATA,
        CONNECTED,
        DISCONNECTED,
        DATA_END_MESSAGE,
        LOAD_RESULT
    }
}
