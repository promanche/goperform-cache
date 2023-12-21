package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.authService.requests.JwtRequest;
import ru.geosteering.commonModels.authService.requests.UserInfoRequest;
import ru.geosteering.commonModels.authService.responses.UserInfo;
import ru.geosteering.commonModels.webService.JSTreeResponse;
import ru.geosteering.commonModels.webService.requests.GetObjectsRequest;
import ru.geosteering.commonModels.webService.responses.ApiResult;
import ru.geosteering.commonModels.webService.responses.ObjectInfoResponse;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.exception.NullResponseException;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class GoStreamClient {

    private final Config config;
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(10);
    private static Connection connection;

    @EventListener(ApplicationStartedEvent.class)
    private void initConnection() {
        connect();
    }

    private void connect() {

        Options options = new Options.Builder()
                .connectionName("goperform-cache-1")
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

    public Map<JSTreeResponse, List<Long>> getAllWellsCurves() throws InterruptedException {
        String replyToSuffix = NUID.nextGlobal();
        String uid = getUserInfo().getUid();
        Map<JSTreeResponse, List<Long>> wellCurves = new HashMap<>();

        GetObjectsRequest request = new GetObjectsRequest();
        request.setAll(false);
        request.setUserUid(uid);
        request.setReplyToSuffix(replyToSuffix);

        List<JSTreeResponse> wells = getObjects(request);
        int curvesCount = 0;
        for (JSTreeResponse well : wells) {
            GetObjectsRequest wellObjectsRequest = new GetObjectsRequest();
            wellObjectsRequest.setAll(true);
            wellObjectsRequest.setUserUid(uid);
            wellObjectsRequest.setReplyToSuffix(replyToSuffix);
            wellObjectsRequest.setParentId(Long.valueOf(well.getId()));

            long started = System.currentTimeMillis();
            List<JSTreeResponse> wellObjects = getObjects(wellObjectsRequest);
            log.debug("Well {} objects received {} ms", well.getId(), System.currentTimeMillis() - started);
            List<Long> curves = wellObjects.stream().filter(jsTreeResponse -> jsTreeResponse.getType().equals("CURVE"))
                    .map(jsTreeResponse -> Long.parseLong(jsTreeResponse.getId())).toList();
            curvesCount += curves.size();
            wellCurves.put(well, curves);
        }
        log.info("{} curves received for {} wells", curvesCount, wells.size());
        return wellCurves;
    }

    private List<JSTreeResponse> getObjects(GetObjectsRequest request) throws InterruptedException {
        Subscription sub = connection.subscribe(config.OBJECTS + '.' + request.getReplyToSuffix());
        try {
            log.info("Requesting: {}", request);

            byte[] requestBytes = StaticMapper.toBytes(request);
            Message replyMsg = connection.request(config.OBJECTS, requestBytes, RESPONSE_TIMEOUT);
            if (replyMsg == null) {
                throw new RuntimeException("No status response from NATS service");
            }
 
            ObjectInfoResponse statusResponse = StaticMapper.parseObject(
                    new String(replyMsg.getData(), StandardCharsets.UTF_8), ObjectInfoResponse.class);
            log.info("Reply: {}", statusResponse);
            if (!EResult.OK.equals(statusResponse.getStatus())) {
                throw new RuntimeException("Error response from NATS service: " + statusResponse);
            }

            List<JSTreeResponse> result = new ArrayList<>();
            if (statusResponse.getObjectCount() == 0){
                return result;
            }
            for (; ; ) {
                Message nextMsg = sub.nextMessage(RESPONSE_TIMEOUT);
                if(nextMsg != null){
                    String messageString = new String(nextMsg.getData(), StandardCharsets.UTF_8);
                    JSTreeResponse jsTreeResponse = StaticMapper.parseObject(messageString, JSTreeResponse.class);
                    if (jsTreeResponse != null){
                        log.trace("Response: {}", jsTreeResponse);
                        result.add(jsTreeResponse);
                    }else {
                        log.info("Objects received: {}", result.size());
                        return result;
                    }
                }
            }
        } finally {
            sub.unsubscribe();
        }
    }


    public String getToken() throws InterruptedException {
        JwtRequest request = new JwtRequest();
        request.setAction("getToken");
        request.setUsername(config.GOSTREAM_USERNAME);
        request.setPassword(config.GOSTREAM_PASSWORD);
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

        log.info("UserInfo Request: {}", request);

        byte[] bytes = StaticMapper.toBytes(request);
        Message message = connection.request(config.AUTH, bytes, RESPONSE_TIMEOUT);
        if (message == null) {
            log.error("UserInfo is null");
            throw new NullResponseException();
        }
        log.info("UserInfo Response: {}", new String(message.getData()));
        ApiResult apiResult = StaticMapper.parseObject(new String(message.getData()), ApiResult.class);
        return StaticMapper.parseObject(StaticMapper.toJson(apiResult.getResult()), UserInfo.class);
    }
}
