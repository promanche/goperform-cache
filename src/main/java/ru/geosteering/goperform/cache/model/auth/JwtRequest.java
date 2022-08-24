package ru.geosteering.goperform.cache.model.auth;

import lombok.*;

@ToString(callSuper = true)
@Getter
@Setter
public class JwtRequest extends Request {
    private final String username;    // логин
    private final String password;    // пароль

    public JwtRequest(String username, String password) {
        super("getToken");
        this.username = username;
        this.password = password;
    }
}
