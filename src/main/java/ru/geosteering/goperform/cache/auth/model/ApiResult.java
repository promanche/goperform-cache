package ru.geosteering.goperform.cache.auth.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ApiResult {
    private EResult status;    // статус выполнения запроса
    private String message;    // дополнительное сообщение (в случае ошибки)
    private Object result;     // результат

    public enum EResult {
        OK,                // успешно
        ERR_ACCESS,        // ошибка доступа
        ERR_OTHER          // другая ошибка
    }
}
