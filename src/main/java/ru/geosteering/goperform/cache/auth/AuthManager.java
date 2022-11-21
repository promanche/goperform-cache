package ru.geosteering.goperform.cache.auth;

import io.nats.client.Message;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.authorization.AuthorizationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.access.intercept.RequestAuthorizationContext;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.model.auth.*;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

@Component
@Slf4j
@RequiredArgsConstructor
public class AuthManager implements AuthorizationManager<RequestAuthorizationContext> {

    private final ObjectAccessor objectAccessor;

    @Override
    public AuthorizationDecision check(Supplier<Authentication> authentication, RequestAuthorizationContext object) {

        try {
            Authentication auth = authentication.get();
            long id = Long.parseLong(object.getVariables().get("id"));

            String method = object.getRequest().getMethod().toUpperCase();

            switch (method) {
                case "GET":
                    return new AuthorizationDecision(checkObjectReadAccess(auth, id));
                case "POST":
                case "PUT":
                case "DELETE":
                    return new AuthorizationDecision(checkObjectWriteAccess(auth, id));
                default:
                    return new AuthorizationDecision(false);
            }

        } catch (NumberFormatException e) {
            return new AuthorizationDecision(false);
        }
    }

    public boolean checkObjectReadAccess(Authentication auth, long id) {
        log.debug("checkObjectReadAccess started. User: {}, id {}", auth.getName(), id);
        boolean result = objectAccessor.check(auth, id, CheckObjectAccessRequest.Permissions.READ);

        if (result) {
            log.debug("checkObjectReadAccess completed. User: {}, id {}, result: {}", auth.getName(), id, true);
        } else {
            log.warn("checkObjectReadAccess completed. User: {}, id {}, result: {}", auth.getName(), id, false);
        }
        return result;
    }

    public boolean checkObjectWriteAccess(Authentication auth, long id) {
        log.debug("checkObjectWriteAccess START. User: {}, id {}", auth.getName(), id);
        boolean result = objectAccessor.check(auth, id, CheckObjectAccessRequest.Permissions.WRITE);

        if (result) {
            log.debug("checkObjectWriteAccess completed. User: {}, id {}, result: {}", auth.getName(), id, true);
        } else {
            log.warn("checkObjectWriteAccess completed. User: {}, id {}, result: {}", auth.getName(), id, false);
        }
        return result;
    }

    public boolean checkBatchReadAccess(Authentication auth, long[] ids) {
        log.debug("checkBatchReadAccess for {}: {}", auth.getName(), ids);
        boolean result = true;
        for (long id : ids) {
            if (!checkObjectReadAccess(auth, id)) {
                result = false;
                break;
            }
        }

        if (result) {
            log.debug("checkBatchReadAccess: permission granted for {}", auth.getName());
        } else {
            log.warn("checkBatchReadAccess: permission denied for {}", auth.getName());
        }
        return result;
    }

    @Cacheable("authentication")
    public Authentication getAuthentication(String jwt) {

        if (jwt == null || jwt.isEmpty() || jwt.equalsIgnoreCase("null")) {
            return null;
        }

        UserAuthentication authentication = null;

        TokenRequest request = new TokenRequest(jwt);

        log.trace("Request: {}", request);
        Message response = NatsConnector.sendRequest("gostream.auth", StaticMapper.toBytes(request));
        log.trace("Response: {}", response);

        ApiResult apiResult = StaticMapper.parseObject(new String(response.getData()), ApiResult.class);

        if (apiResult != null && apiResult.getStatus() == ApiResult.EResult.OK) {

            authentication = new UserAuthentication();
            authentication.setUserName((String) apiResult.getResult());
            authentication.setToken(jwt);
            authentication.setAuthority(new SimpleGrantedAuthority("ROLE_USER"));
            authentication.setAuthenticated(true);
        }

        return authentication;
    }

    @Setter
    @ToString
    private static class UserAuthentication implements Authentication {

        private String userName;
        private String token;
        private SimpleGrantedAuthority authority;
        private boolean authenticated;

        @Override
        public Collection<? extends GrantedAuthority> getAuthorities() {
            return authority == null ? Collections.emptySet() : Collections.singleton(authority);
        }

        @Override
        public Object getCredentials() {
            return token;
        }

        @Override
        public Object getDetails() {
            return null;
        }

        @Override
        public Object getPrincipal() {
            return userName;
        }

        @Override
        public boolean isAuthenticated() {
            return authenticated;
        }

        @Override
        public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
            this.authenticated = isAuthenticated;
        }

        @Override
        public String getName() {
            return userName;
        }
    }
}
