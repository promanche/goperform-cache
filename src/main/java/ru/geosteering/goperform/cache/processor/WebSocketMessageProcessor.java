package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class WebSocketMessageProcessor implements DefaultEventProcessor {

    private final SimpMessagingTemplate template;
    private final SimpUserRegistry userRegistry;

    @Override
    public void onRealtimeCurveItem(Long id, CurveItem item) {
        String toWs = "{\"id\":" + id + ",\"point\":" + StaticMapper.toJson(item) + "}";
        template.convertAndSend("/curve/" + id + "/new-point", toWs);
    }

    @Override
    public void onItemsBatch(Long id, List<CurveItem> items) {
        DefaultEventProcessor.super.onItemsBatch(id, items);
    }

    @Override
    public void onLoadResult(Long id, LoadResult result) {
        if (result == LoadResult.DONE) {
            template.convertAndSend("/curve/" + id + "/loaded", "{\"curveId\":" + id + "}");
        }
    }

    @Scheduled(fixedDelay = 60, timeUnit = TimeUnit.SECONDS)
    private void heartbeat() {

        log.info("WebSocket info: users {}, subscriptions {}",
                userRegistry.getUserCount(), userRegistry.findSubscriptions(subscription -> true).size());
    }
}
