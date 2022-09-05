package ru.geosteering.goperform.cache.model.auth;

import lombok.*;

@ToString
@Getter
@Setter
public class Request {
    private final String action;      // тип запроса

    public Request(String action) {
        this.action = action;
    }
}
