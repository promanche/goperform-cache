package ru.geosteering.goperform.cache.auth.model;

import lombok.*;

@ToString(callSuper = true)
@Getter
@Setter
public class TokenRequest extends Request {
    private final String token;       // jwt токен

    public TokenRequest(String token) {
        super("validateToken");
        this.token = token;
    }
}
