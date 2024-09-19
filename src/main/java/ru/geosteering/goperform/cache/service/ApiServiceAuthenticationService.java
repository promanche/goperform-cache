package ru.geosteering.goperform.cache.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import ru.geosteering.goperform.cache.config.Config;

import javax.annotation.Nullable;


@Slf4j
@RequiredArgsConstructor
@Service
public class ApiServiceAuthenticationService {

    private final Config config;
    private final WebClient apiServiceClient;

    /**
     * Этот метод выполняет аутентификацию (сервисная учетная запись в application.properties) и возвращает JWT токен.
     *
     * @return String JWT токен или <code>null</code>
     */
    @Cacheable(value = "apiServiceToken", unless = "#result == null")
    @Nullable
    public String authenticate() {
        JwtResponse response = apiServiceClient
                .post()
                .uri("/auth")
                .body(Mono.just(JwtRequest.builder()
                                .username(config.GOSTREAM_USERNAME)
                                .password(config.GOSTREAM_PASSWORD)
                                .build()), JwtRequest.class)
                .retrieve()
                .bodyToMono(JwtResponse.class)
                .block();
        if (response == null) {
            return null;
        }
        return response.getToken();
    }
    @Getter
    @Setter
    @Builder
    private static class JwtRequest{
        private String username;
        private String password;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @Builder
    @ToString
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class JwtResponse{
        @JsonProperty("jwttoken")
        String token;
    }
}
