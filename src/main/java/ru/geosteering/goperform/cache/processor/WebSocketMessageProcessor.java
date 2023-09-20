package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.model.ws.WsMessage;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class WebSocketMessageProcessor {

    private final SimpMessagingTemplate template;
    private final SimpUserRegistry userRegistry;

    public void sendMessage(Long id, WsMessage message) {
        template.convertAndSend("/curve/" + id, StaticMapper.toJson(message));
    }

    @Scheduled(fixedDelay = 60, timeUnit = TimeUnit.SECONDS)
    private void heartbeat() {

        log.info("WebSocket info: users {}, subscriptions {}",
                userRegistry.getUserCount(), userRegistry.findSubscriptions(subscription -> true).size());
    }
}
