package ru.geosteering.goperform.cache.nats;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class ConnectionEventDispatcher {

    private final Set<ConnectionEventListener> set = new HashSet<>();

    public ConnectionEventDispatcher(@Autowired List<ConnectionEventListener> list) {
        set.addAll(list);
    }

    public void onConnect() {
        set.forEach(ConnectionEventListener::onConnect);
    }

    public void onDisconnect() {
        set.forEach(ConnectionEventListener::onDisconnect);
    }
}
