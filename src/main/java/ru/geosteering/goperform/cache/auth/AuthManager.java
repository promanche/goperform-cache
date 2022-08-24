package ru.geosteering.goperform.cache.auth;

import io.nats.client.Message;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.authorization.AuthorizationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.access.intercept.RequestAuthorizationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import ru.geosteering.goperform.cache.model.auth.*;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

@Component
@Slf4j
public class AuthManager extends OncePerRequestFilter implements AuthorizationManager<RequestAuthorizationContext> {

    private final Map<String, UserAuthentication> authenticatedUsers = new ConcurrentHashMap<>();

    @Override
    public AuthorizationDecision check(Supplier<Authentication> authentication, RequestAuthorizationContext object) {

        Authentication auth = authentication.get();
        long id = Long.parseLong(object.getRequest().getServletPath().split("/")[2]);

        return new AuthorizationDecision(checkObjectAccess(auth, id));
    }

    public boolean checkObjectAccess(Authentication auth, long id) {

        String userName = auth.getName();

        if (
                userName == null
                        || userName.isEmpty()
                        || userName.equalsIgnoreCase("anonymousUser")
                        || userName.equalsIgnoreCase("anonymous")
        ) {

            return false;
        }

        CheckObjectAccessRequest request = new CheckObjectAccessRequest(userName, id, CheckObjectAccessRequest.Permissions.READ);

        Message response = NatsConnector.sendRequest("gostream.auth", StaticMapper.toBytes(request));

        ApiResult apiResult = StaticMapper.parseObject(new String(response.getData()), ApiResult.class);

        return apiResult != null && apiResult.getStatus() == ApiResult.EResult.OK;
    }

    public Authentication getAuthentication(String jwt) {

        if (jwt == null || jwt.isEmpty()) {
            return null;
        }

        UserAuthentication authentication = null;

        if (authenticatedUsers.containsKey(jwt)) {

            authentication = authenticatedUsers.get(jwt);

        } else {

            TokenRequest request = new TokenRequest(jwt);

            Message response = NatsConnector.sendRequest("gostream.auth", StaticMapper.toBytes(request));

            ApiResult apiResult = StaticMapper.parseObject(new String(response.getData()), ApiResult.class);

            if (apiResult != null && apiResult.getStatus() == ApiResult.EResult.OK) {

                authentication = new UserAuthentication();
                authentication.setUserName((String) apiResult.getResult());
                authentication.setToken(jwt);
                authentication.setAuthority(new SimpleGrantedAuthority("ROLE_USER"));
                authentication.setAuthenticated(true);

                authenticatedUsers.putIfAbsent(jwt, authentication);
            }
        }

        return authentication;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {

        String authHeader = request.getHeader(HttpHeaders.AUTHORIZATION);
        String jwt = null;

        if (authHeader != null && authHeader.startsWith("Bearer")) {
            jwt = authHeader.substring(6).trim();
        }

        Authentication authentication = getAuthentication(jwt);

        if (authentication != null) {
            SecurityContext context = SecurityContextHolder.createEmptyContext();
            context.setAuthentication(authentication);
            SecurityContextHolder.setContext(context);
        }

        filterChain.doFilter(request, response);
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
