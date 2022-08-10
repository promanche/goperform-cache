package ru.geosteering.goperform.cache.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.FilterSecurityInterceptor;
import ru.geosteering.goperform.cache.auth.AuthManager;

@Configuration
@EnableWebSecurity
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
                                .antMatchers("/curve/**").access(authManager)
                                .anyRequest().denyAll()
                )
                .addFilterBefore(authManager, FilterSecurityInterceptor.class);

        return http.build();
    }
}
