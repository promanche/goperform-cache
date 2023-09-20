package ru.geosteering.goperform.cache.nats;

public interface ConnectionEventListener {

    void onConnect();

    void onDisconnect();
}
