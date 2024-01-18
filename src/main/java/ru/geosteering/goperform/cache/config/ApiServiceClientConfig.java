package ru.geosteering.goperform.cache.config;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

import javax.net.ssl.SSLException;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class ApiServiceClientConfig {
    private final Config config;

    @Bean("apiServiceClient")
    public WebClient apiServiceClient(WebClient.Builder builder) {
        try {
            SslContext sslContext = SslContextBuilder
                    .forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            return builder
                    .clientConnector(new ReactorClientHttpConnector(HttpClient.from(TcpClient.create())
                            .secure(sslContextSpec -> sslContextSpec.sslContext(sslContext))))
                    .baseUrl(config.DATA_SERVICE_BASEURL)
                    .build();
        } catch (SSLException e) {
            log.warn("SSL context create error", e);
            return null;
        }
    }
}
