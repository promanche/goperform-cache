package ru.geosteering.goperform.cache.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.FilterSecurityInterceptor;
import org.springframework.security.web.session.DisableEncodeUrlFilter;
import org.springframework.web.filter.CommonsRequestLoggingFilter;
import ru.geosteering.goperform.cache.auth.AuthManager;

@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
@RequiredArgsConstructor
public class SecurityConfig {

    private final AuthManager authManager;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
                .csrf().disable()
                .authorizeHttpRequests(
                        (auth) -> auth
                                .antMatchers("/ws").permitAll()
                                .antMatchers( "/curve/multi/**").hasRole("USER")
                                .antMatchers(HttpMethod.POST, "/curve").hasRole("USER")
                                .antMatchers(HttpMethod.DELETE, "/curve/{id}/coordinates/by-time", "/curve/{id}/coordinates/by-depth").hasRole("USER")
                                .antMatchers("/curve/{id}/**").access(authManager)
                                .anyRequest().denyAll()
                )
                .addFilterBefore(authManager, FilterSecurityInterceptor.class)
                .addFilterBefore(requestLoggingFilter(), DisableEncodeUrlFilter.class);

        return http.build();
    }

    @Bean
    public CommonsRequestLoggingFilter requestLoggingFilter() {
        CommonsRequestLoggingFilter loggingFilter = new CommonsRequestLoggingFilter();
        loggingFilter.setIncludeClientInfo(true);
        loggingFilter.setIncludeQueryString(true);
        loggingFilter.setIncludeHeaders(true);
        loggingFilter.setIncludePayload(true);
        loggingFilter.setMaxPayloadLength(256);
        return loggingFilter;
    }
}
