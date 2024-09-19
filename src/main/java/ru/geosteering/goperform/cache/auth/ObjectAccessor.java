package ru.geosteering.goperform.cache.auth;

import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.TLUserObjectIn;
import ru.geosteering.commonModels.authService.requests.CheckObjectAccessRequest;
import ru.geosteering.commonModels.webService.responses.ApiResult;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.utils.StaticMapper;

@Component
@Slf4j
@RequiredArgsConstructor
public class ObjectAccessor {

    /**
     * Порог логирования: проверки доступа, проведённые быстрее данного порога, не должны логироваться, чтобы не захламлять лог.
     */
    public static final long LOG_THRESHOLD_MILLIS = 100;
    private final Config config;

    @Cacheable(value = "objectAccess", unless = "#result == false", key = "#username + #id + #permission.toString()")
    public boolean check(String username, long id, TLUserObjectIn.Permissions permission) {

        try {
            CheckObjectAccessRequest request = new CheckObjectAccessRequest();
            request.setAction("checkObjectAccess");
            request.setUsername(username);
            request.setObjectId(id);
            request.setPermission(permission);

            log.trace("Request: {}", request);
            Message response = NatsConnector.sendRequest(config.GOSTREAM_AUTH, StaticMapper.toBytes(request));
            log.trace("Response: {}", response);

            ApiResult apiResult = StaticMapper.parseObject(new String(response.getData()), ApiResult.class);

            return apiResult != null && apiResult.getStatus() == EResult.OK;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }
}
