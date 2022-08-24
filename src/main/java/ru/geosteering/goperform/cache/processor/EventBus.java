package ru.geosteering.goperform.cache.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.event.Event;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
public class EventBus {

    private final Map<Event.EventType, List<EventProcessor>> dispatcher;
    private final ExecutorService worker;

    public EventBus(Config config) {
        this.dispatcher = new HashMap<>();
        this.worker = Executors.newFixedThreadPool(config.HISTORY_THREADS + config.REALTIME_THREADS + 1);
    }

    public void register(List<Event.EventType> events, EventProcessor eventProcessor) {

        synchronized (dispatcher) {
            events.forEach(e -> dispatcher.computeIfAbsent(e, k -> new ArrayList<>())
                    .add(eventProcessor));
        }

        log.info("{} registered as processor for {} events", eventProcessor.getClass().getSimpleName(), events);
    }

    public Future<?> post(Event event) {

        return worker.submit(() -> dispatcher.get(event.getType()).forEach(processor -> {
            try {
                processor.processEvent(event);
            } catch (Exception e) {
                log.error("{} exception: {}", processor.getClass().getSimpleName(), e.getMessage(), e);
            }
        }));
    }

    @PreDestroy
    private void stop() {
        try {
            worker.shutdown();

            if (!worker.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
                log.warn(getClass().getSimpleName() + " worker shutdown timeout");
            } else {
                log.info(getClass().getSimpleName() + " worker shutdown");
            }

        } catch (InterruptedException e) {
            log.error(getClass().getSimpleName() + "worker shutdown exception: {}", e.getMessage(), e);
        }
    }
}
