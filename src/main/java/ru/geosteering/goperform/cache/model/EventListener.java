package ru.geosteering.goperform.cache.model;

import ru.geosteering.goperform.cache.model.event.Event;

public interface EventListener {

    void onEvent(Event<?> event);
}
