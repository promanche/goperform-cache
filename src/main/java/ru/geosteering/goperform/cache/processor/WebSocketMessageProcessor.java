package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.ws.*;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class WebSocketMessageProcessor implements DefaultEventProcessor {

    private final SimpMessagingTemplate template;
    private final SimpUserRegistry userRegistry;

    @Override
    public void onRealtimeCurveItem(Long id, CurveItem item) {
        PointMessage message = new PointMessage(id, item.getKey(), item.getValue());
        template.convertAndSend("/curve/" + id, StaticMapper.toJson(message));
    }

    @Override
    public void onLoadResult(Long id, LoadResult result, Double from, Double to) {

        switch (result) {
            case DONE:
                LoadedMessage messageL = new LoadedMessage(id);
                template.convertAndSend("/curve/" + id, StaticMapper.toJson(messageL));
                break;
            case PART:
                PartMessage messageB = new PartMessage(id, from, to);
                template.convertAndSend("/curve/" + id, StaticMapper.toJson(messageB));
                break;
            default:
                break;
        }
    }

    @Scheduled(fixedDelay = 60, timeUnit = TimeUnit.SECONDS)
    private void heartbeat() {

        log.info("WebSocket info: users {}, subscriptions {}",
                userRegistry.getUserCount(), userRegistry.findSubscriptions(subscription -> true).size());
    }
}
