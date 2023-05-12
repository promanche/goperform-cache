package ru.geosteering.goperform.cache.nats;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ConnectionEventDispatcher {

    private final List<ConnectionEventListener> listeners;

    public ConnectionEventDispatcher(@Autowired List<ConnectionEventListener> listeners) {
        this.listeners = listeners;
    }

    public synchronized void onConnect() {
        listeners.forEach(ConnectionEventListener::onConnect);
    }

    public synchronized void onDisconnect() {
        listeners.forEach(ConnectionEventListener::onDisconnect);
    }
}
