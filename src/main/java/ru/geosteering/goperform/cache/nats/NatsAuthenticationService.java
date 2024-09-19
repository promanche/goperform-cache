package ru.geosteering.goperform.cache.nats;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.authService.requests.JwtRequest;
import ru.geosteering.commonModels.authService.requests.UserInfoRequest;
import ru.geosteering.commonModels.authService.responses.UserInfo;
import ru.geosteering.commonModels.webService.responses.ApiResult;
import ru.geosteering.goperform.cache.config.Config;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
@RequiredArgsConstructor
public class NatsAuthenticationService {
    private final ObjectMapper mapper = new ObjectMapper();

    private final Config config;


    /**
     * Метод получения JWT token.
     * @return JWT token.
     */

    public String getToken() {
        try {
            JwtRequest jwtRequest = new JwtRequest();
            jwtRequest.setAction("getToken");
            jwtRequest.setUsername(config.GOSTREAM_USERNAME);
            jwtRequest.setPassword(config.GOSTREAM_PASSWORD);

            String request = mapper.writeValueAsString(jwtRequest);
            Message msg = NatsConnector.sendRequest(config.GOSTREAM_AUTH, request.getBytes(StandardCharsets.UTF_8));
            String response = msg == null ? null : new String(msg.getData(), StandardCharsets.UTF_8);
            if (response != null && !response.isEmpty()) {
                ApiResult apiResult = mapper.readValue(response, ApiResult.class);
                if (apiResult.getStatus() == EResult.OK) {
                    return (String) apiResult.getResult();
                } else {
                    throw new Exception("Failed to get jwt token -" + apiResult.getMessage());
                }
            } else {
                throw new Exception("Failed to get jwt token - no response from NATS AuthService");
            }
        } catch (InterruptedException e) {
            log.error("NATS request exception: {}", e.getMessage());
        } catch (Throwable e) {
            log.error("Exception: ", e);
        }
        return null;
    }


    /**
     * Этот метод получает информацию о пользователе.
     * @return информация о пользователе.
     */
    public UserInfo getUserInfo() {
        try {
            UserInfoRequest userInfoRequest = new UserInfoRequest();
            userInfoRequest.setAction("userInfo");
            userInfoRequest.setToken(getToken());
            String request = mapper.writeValueAsString(userInfoRequest);
            Message msg = NatsConnector.sendRequest(config.GOSTREAM_AUTH, request.getBytes(StandardCharsets.UTF_8));
            if (msg == null) {
                throw new Exception("Failed to get user info - no response from NATS AuthService");
            }

            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            ApiResult apiResult = mapper.readValue(response, ApiResult.class);
            if (apiResult.getStatus() == EResult.OK) {
                return mapper.readValue(mapper.writeValueAsString(apiResult.getResult()), UserInfo.class);
            } else {
                throw new Exception("Failed to get user info - " + apiResult.getMessage());
            }
        } catch (InterruptedException e) {
            log.error("NATS request exception: {}", e.getMessage());
        } catch (Throwable e) {
            log.error("Exception: ", e);
        }
        return null;
    }
}
