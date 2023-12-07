package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.authService.requests.JwtRequest;
import ru.geosteering.commonModels.authService.requests.UserInfoRequest;
import ru.geosteering.commonModels.authService.responses.UserInfo;
import ru.geosteering.commonModels.dataService.requests.WellGetRequest;
import ru.geosteering.commonModels.dataService.responses.DataEndMessage;
import ru.geosteering.commonModels.dataService.responses.StatusMessage;
import ru.geosteering.commonModels.dataService.responses.WellInfoMessage;
import ru.geosteering.commonModels.webService.JSTreeResponse;
import ru.geosteering.commonModels.webService.requests.GetObjectsRequest;
import ru.geosteering.commonModels.webService.requests.GetTabletObjectsRequest;
import ru.geosteering.commonModels.webService.requests.ObjectInfoRequest;
import ru.geosteering.commonModels.webService.responses.ApiResult;
import ru.geosteering.commonModels.webService.responses.ObjectInfoResponse;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.exception.NullResponseException;
import ru.geosteering.goperform.cache.utils.StaticMapper;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.ObjWell;
import ru.geosteering.witsmlLibrary.witsml.dataObjs.ObjWellbore;

import javax.annotation.PreDestroy;
import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class ApiServiceDataClient {

    private final Config config;
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(10);

    private static Connection connection;

    @EventListener(ApplicationStartedEvent.class)
    private void initConnection() {
        connect();
    }

    private void connect() {

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .noReconnect()
                .errorListener(new ErrorListenerLoggerImpl())
                .authHandler(Nats.credentials(config.CREDENTIALS_FILE))
                .server(config.HOST)
                .build();

        try {
            connection = Nats.connect(options);
        } catch (IOException | InterruptedException e) {
            log.error("Connection exception: {}", e.getMessage(), e);
        }
    }

    private static class ErrorListenerLoggerImpl extends io.nats.client.impl.ErrorListenerLoggerImpl {
        @Override
        public void exceptionOccurred(final Connection conn, final Exception exp) {
            log.error("NATS exception occurred", exp);
        }
    }

    public static boolean isConnected() {
        return connection != null && connection.getStatus() == Connection.Status.CONNECTED;
    }

    @PreDestroy
    private void closeConnection() {
        try {
            connection.close();
        } catch (Exception e) {
            log.error("Exception while closing connection: {}", e.getMessage(), e);
        }
    }

    public List<JSTreeResponse> getAllObjects() throws InterruptedException {
        String replyToSuffix = NUID.nextGlobal();

        GetObjectsRequest request = new GetObjectsRequest();
        request.setParentId(null);
        request.setAll(true);
        request.setUserUid("d2b5cf44-19da-11eb-9e91-1f146a486534");
        request.setReplyToSuffix(replyToSuffix);

        Subscription sub = connection.subscribe(config.OBJECTS + ".*" + replyToSuffix);
        try {
            log.debug("Requesting {}", request);


            byte[] requestBytes = StaticMapper.toBytes(request);
            Message replyMsg = connection.request(config.OBJECTS, requestBytes, RESPONSE_TIMEOUT);
            if (replyMsg == null) {
                throw new RuntimeException("No status response from NATS service");
            }
 
            ObjectInfoResponse statusResponse = StaticMapper.parseObject(
                    new String(replyMsg.getData(), StandardCharsets.UTF_8), ObjectInfoResponse.class);
            log.trace("Reply: {}", statusResponse);
            if (!EResult.OK.equals(statusResponse.getStatus())) {
                throw new RuntimeException("Error response from NATS service: " + statusResponse);
            }

            List<JSTreeResponse> result = new ArrayList<>();

            for (; ; ) {
                Message nextMsg = sub.nextMessage(RESPONSE_TIMEOUT);
                if (nextMsg == null) {
                    throw new RuntimeException("No data response from NATS service");
                }
                String messageString = new String(nextMsg.getData(), StandardCharsets.UTF_8);

                JSTreeResponse jsTreeResponse = StaticMapper.parseObject(messageString, JSTreeResponse.class);
                log.trace("Response {}", jsTreeResponse);
                result.add(jsTreeResponse);

                if (messageString.contains("\"type\":\"end\"")) {
                    DataEndMessage msg = StaticMapper.parseObject(messageString, DataEndMessage.class);
                    if (result.size() != msg.getSentCount()) {
                        throw new RuntimeException("Received " + result.size() + " objects expected " + msg.getSentCount());
                    }
                    log.info("Objects received: {}", result.size());
                    return result;
                }
            }
        } finally {
            sub.unsubscribe();
        }

    }

    public String getToken() throws InterruptedException {
        JwtRequest request = new JwtRequest();
        request.setAction("getToken");
        request.setUsername(config.NATS_USERNAME);
        request.setPassword(config.NATS_PASSWORD);
        log.info("JWT Token Request: {}", request);

        byte[] bytes = StaticMapper.toBytes(request);
        Message message = connection.request(config.AUTH, bytes, RESPONSE_TIMEOUT);
        if (message == null) {
            log.error("JWT Token is null");
            throw new NullResponseException();
        }
        log.info("JWT Token Response: {}", new String(message.getData()));
        ApiResult apiResult = StaticMapper.parseObject(new String(message.getData()), ApiResult.class);
        return (String) apiResult.getResult();
    }

    public UserInfo getUserInfo() throws InterruptedException {
        UserInfoRequest request = new UserInfoRequest();
        request.setAction("userInfo");
        request.setToken(getToken());

        //request.setId();
        log.info("UserInfo Request: {}", request);

        byte[] bytes = StaticMapper.toBytes(request);
        Message message = connection.request(config.AUTH, bytes, RESPONSE_TIMEOUT);
        if (message == null) {
            log.error("UserInfo is null");
            throw new NullResponseException();
        }
        log.info("UserInfo Response: {}", new String(message.getData()));
        return StaticMapper.parseObject(new String(message.getData()), UserInfo.class);
    }
}
