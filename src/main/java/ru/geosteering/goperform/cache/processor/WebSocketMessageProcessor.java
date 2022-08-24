package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.event.Event;
import ru.geosteering.goperform.cache.model.event.apimessage.DataEndMessage;
import ru.geosteering.goperform.cache.model.event.item.ItemsBatch;
import ru.geosteering.goperform.cache.model.event.item.RealtimeItem;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class WebSocketMessageProcessor implements EventProcessor {

    private final SimpMessagingTemplate template;
    private final SimpUserRegistry userRegistry;
    private final EventBus eventBus;

    @PostConstruct
    private void register() {
        eventBus.register(
                List.of(
                        Event.EventType.REALTIME_ITEM,
                        Event.EventType.ITEMS_BATCH,
                        Event.EventType.DATA_END_MESSAGE
                ),
                this);
    }

    @Override
    public void processEvent(Event event) {

        Event.EventType eventType = event.getType();

        switch (eventType) {
            case REALTIME_ITEM:
                onNewItem((RealtimeItem) event);
                break;
            case ITEMS_BATCH:
                onBatchCollected((ItemsBatch) event);
                break;
            case DATA_END_MESSAGE:
                onEndMessage((DataEndMessage) event);
                break;
            default:
                break;
        }
    }

    private void onNewItem(RealtimeItem event) {
        Long id = event.getId();
        CurveItem item = event.getCurveItem();

        String toWs = "{\"id\":" + id + ",\"point\":" + StaticMapper.toJson(item) + "}";
        template.convertAndSend("/curve/" + id + "/new-point", toWs);
    }

    private void onBatchCollected(ItemsBatch event) {
    }

    private void onEndMessage(DataEndMessage event) {

        if (event.getResult() == DataEndMessage.Result.DONE) {
            Long id = event.getId();
            template.convertAndSend("/curve/" + id + "/loaded", "{\"curveId\":" + id + "}");
        }
    }

    @Scheduled(fixedDelay = 60, timeUnit = TimeUnit.SECONDS)
    private void heartbeat() {

        log.info("WebSocket info: users {}, subscriptions {}",
                userRegistry.getUserCount(), userRegistry.findSubscriptions(subscription -> true).size());
    }
}
