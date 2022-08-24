package ru.geosteering.goperform.cache.processor;

import ru.geosteering.goperform.cache.model.event.Event;

public interface EventProcessor {

    void processEvent(Event event);
}
