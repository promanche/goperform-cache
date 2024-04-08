package ru.geosteering.goperform.cache.auth;

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
import ru.geosteering.commonModels.TLUserObjectIn;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.nats.NatsAuthenticationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

@Component
@Slf4j
@RequiredArgsConstructor
public class AuthManager implements AuthorizationManager<RequestAuthorizationContext> {

    private final Config config;
    private final ObjectAccessor objectAccessor;
    private final NatsAuthenticationService authenticationService;


    @Override
    public AuthorizationDecision check(Supplier<Authentication> authentication, RequestAuthorizationContext object) {

        try {
            long id = Long.parseLong(object.getVariables().get("id"));

            String method = object.getRequest().getMethod().toUpperCase();

            return switch (method) {
                case "GET" -> new AuthorizationDecision(checkObjectReadAccess(id));
                case "POST", "PUT", "DELETE" -> new AuthorizationDecision( checkObjectWriteAccess(id));
                default -> new AuthorizationDecision(false);
            };

        } catch (NumberFormatException e) {
            return new AuthorizationDecision(false);
        }
    }

    public boolean checkObjectAccess(long id, TLUserObjectIn.Permissions permit) {
        long startMillis = System.currentTimeMillis();
        boolean result = objectAccessor.check(config.GOSTREAM_USERNAME, id, permit);

        long endMillis = System.currentTimeMillis();
        if (!result) {
            log.warn("checkObjectAccess( {}, {}, {} ): access denied in {} ms", config.GOSTREAM_USERNAME, id, permit, endMillis - startMillis);
        } else if (endMillis - startMillis > ObjectAccessor.LOG_THRESHOLD_MILLIS) {
            log.info("checkObjectAccess( {}, {}, {} ): took too long, duration {} ms", config.GOSTREAM_USERNAME, id, permit, endMillis - startMillis);
        }

        return result;
    }

    public boolean checkObjectReadAccess(long id) {
        return checkObjectAccess(id, TLUserObjectIn.Permissions.READ);
    }

    public boolean checkObjectWriteAccess(long id) {
        return checkObjectAccess(id, TLUserObjectIn.Permissions.WRITE);
    }

    public boolean checkBatchReadAccess(Authentication auth, Long[] ids) {
        long startMillis = System.currentTimeMillis();
        boolean result = true;
        for (long id : ids) {
            if (!objectAccessor.check(auth.getName(), id, TLUserObjectIn.Permissions.READ)) {
                result = false;
                break;
            }
        }

        long endMillis = System.currentTimeMillis();
        if (!result) {
            log.warn("checkBatchReadAccess( {}, {} ): access denied in {} ms", auth.getName(), ids, endMillis - startMillis);
        } else if (endMillis - startMillis > ObjectAccessor.LOG_THRESHOLD_MILLIS) {
            log.info("checkBatchReadAccess( {}, {} ): took too long, duration {} ms", auth.getName(), ids, endMillis - startMillis);
        }
        return result;
    }

    public Long[] checkBatchDeniedAccess(Long[] ids) {
        List<Long> result = new ArrayList<>();
        for (long id : ids) {
            if (objectAccessor.check(config.GOSTREAM_USERNAME, id, TLUserObjectIn.Permissions.READ)) {
                result.add(id);
            }
        }
        return result.toArray(Long[]::new);
    }


    @Cacheable(value = "authentication", unless = "#result == null")
    public Authentication getAuthentication(String jwt) {

        if (jwt == null || jwt.isEmpty() || jwt.equalsIgnoreCase("null")) {
            return null;
        }

        UserAuthentication authentication = new UserAuthentication();
        authentication.setUserName( config.GOSTREAM_USERNAME);
        authentication.setToken(authenticationService.getToken());
        authentication.setAuthority(new SimpleGrantedAuthority("ROLE_USER"));
        authentication.setAuthenticated(true);

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
