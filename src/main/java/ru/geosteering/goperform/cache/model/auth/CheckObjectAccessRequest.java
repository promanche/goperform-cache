package ru.geosteering.goperform.cache.model.auth;

import lombok.*;

@Getter
@Setter
@ToString(callSuper = true)
public class CheckObjectAccessRequest extends Request {
    private final String username;        // логин
    private final Long objectId;          // id объекта, доступ к которому проверяется
    private final Permissions permission; // проверяемое разрешение

    public CheckObjectAccessRequest(String username, Long objectId, Permissions permission) {
        super("checkObjectAccess");
        this.username = username;
        this.objectId = objectId;
        this.permission = permission;
    }

    public enum Permissions {
        READ,
        WRITE,
        CONTROL
    }
}


