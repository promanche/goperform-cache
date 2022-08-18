package ru.geosteering.goperform.cache.service;

import lombok.extern.slf4j.Slf4j;
import ru.geosteering.goperform.cache.model.EventListener;
import ru.geosteering.goperform.cache.model.event.Event;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class EventBus {

    private static final Map<Event.EventType, List<EventListener>> dispatcher = new HashMap<>();
    private static final ExecutorService worker = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public static void register(List<Event.EventType> events, EventListener eventListener) {

        synchronized (dispatcher) {
            events.forEach(e -> dispatcher.computeIfAbsent(e, k -> new ArrayList<>())
                    .add(eventListener));
        }

        log.info("{} registered as listener for {} events", eventListener.getClass().getSimpleName(), events);
    }

    public static void post(Event<?> event) {

        log.trace("New event: {}", event);

        dispatcher.get(event.getType())
                .forEach(l -> worker.submit(() -> {
                    try {
                        l.onEvent(event);
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }));
    }
}
