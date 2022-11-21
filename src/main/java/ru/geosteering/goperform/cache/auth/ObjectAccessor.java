package ru.geosteering.goperform.cache.auth;

import io.nats.client.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.model.auth.ApiResult;
import ru.geosteering.goperform.cache.model.auth.CheckObjectAccessRequest;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.utils.StaticMapper;

@Component
@Slf4j
public class ObjectAccessor {

    @Cacheable(value = "objectAccess", condition = "#result")
    public boolean check(Authentication auth, long id, CheckObjectAccessRequest.Permissions permission) {

        try {
            String userName = auth.getName();

            if (
                    userName == null
                            || userName.isEmpty()
                            || userName.equalsIgnoreCase("anonymousUser")
                            || userName.equalsIgnoreCase("anonymous")
            ) {

                return false;
            }

            CheckObjectAccessRequest request = new CheckObjectAccessRequest(userName, id, permission);

            log.trace("Request: {}", request);
            String authSubject = "gostream.auth";
            Message response = NatsConnector.sendRequest(authSubject, StaticMapper.toBytes(request));
            log.trace("Response: {}", response);

            ApiResult apiResult = StaticMapper.parseObject(new String(response.getData()), ApiResult.class);

            return apiResult != null && apiResult.getStatus() == ApiResult.EResult.OK;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }
}
