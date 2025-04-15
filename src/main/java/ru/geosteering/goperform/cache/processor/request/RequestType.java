package ru.geosteering.goperform.cache.processor.request;

public enum RequestType {

    INFO_REST(0),
    INFO_ACTIVE(1),
    LOAD_REST(2),
    LOAD_ACTIVE(3),
    RELOAD(4);

    final int priority;

    RequestType(int priority) {
        this.priority = priority;
    }

    public static RequestType higherPriority(RequestType a, RequestType b) {
        return a.priority <= b.priority ? a : b;
    }

}