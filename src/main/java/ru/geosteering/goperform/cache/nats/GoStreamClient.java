package ru.geosteering.goperform.cache.nats;

import io.nats.client.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.authService.requests.JwtRequest;
import ru.geosteering.commonModels.authService.requests.UserInfoRequest;
import ru.geosteering.commonModels.authService.responses.UserInfo;
import ru.geosteering.commonModels.webService.JSTreeResponse;
import ru.geosteering.commonModels.webService.requests.GetObjectRequest;
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
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(30);
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

    public Map<WellState, List<Long>> getAllWellsCurves(List<Long> ids) throws InterruptedException {
        long started = System.currentTimeMillis();
        String replyToSuffix = NUID.nextGlobal();
        String uid = getUserInfo().getUid();

        Map<WellState, List<Long>> wellCurves = new HashMap<>();

        Subscription sub = connection.subscribe(config.OBJECTS + '.' + replyToSuffix);
        try {
            ids.forEach(id -> {

                List<Long> list = wellCurves.values().stream().flatMap(List::stream).toList();
                if (!list.contains(id)) {
                    List<Long> curves = new ArrayList<>();
                    try {
                        JSTreeResponse jsTreeResponse = getObject(sub, id, uid, replyToSuffix);
                        while (!jsTreeResponse.getType().equals("WELL")
                                || jsTreeResponse.getType().equals("WELL_RED")
                                || jsTreeResponse.getType().equals("WELL_YELLOW")
                                || jsTreeResponse.getType().equals("WELL_GREEN")) {

                            jsTreeResponse = getObject(sub, Long.parseLong(jsTreeResponse.getParent()), uid, replyToSuffix);

                            if (jsTreeResponse.getType().equals("WELLBORE")) {
                                List<JSTreeResponse> objectsCurves = getObjects(sub, Long.parseLong(jsTreeResponse.getId()), uid, replyToSuffix, true);
                                List<Long> logCurves = objectsCurves
                                        .stream().filter(object -> object.getType().equals("CURVE"))
                                        .map(object -> Long.parseLong(object.getId())).toList();

                                curves.addAll(logCurves);
                            }
                        }

                        WellState wellState = new WellState(Long.parseLong(jsTreeResponse.getId()), jsTreeResponse.getType());
                        List<Long> listCurves = wellCurves.getOrDefault(wellState, new ArrayList<>());
                        curves.retainAll(ids);
                        listCurves.addAll(curves);
                        wellCurves.put(wellState, listCurves);

                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            });
            log.info("{} curves received for {} wells in {} ms", wellCurves.values().stream().flatMap(List::stream).toList().size(), wellCurves.size(), System.currentTimeMillis() - started);
            return wellCurves;
        } finally {
            sub.unsubscribe();
        }
    }

    private JSTreeResponse getObject(Subscription sub, Long id, String userUid, String replyToSuffix) throws InterruptedException {
        GetObjectRequest objectLogRequest = new GetObjectRequest();
        objectLogRequest.setUserUid(userUid);
        objectLogRequest.setReplyToSuffix(replyToSuffix);
        objectLogRequest.setId(id);
        return getObject(sub, objectLogRequest);
    }

    private List<JSTreeResponse> getObjects(Subscription sub, Long parentId, String userUid, String replyToSuffix, boolean all) throws InterruptedException {
        GetObjectsRequest objectsRequest = new GetObjectsRequest();
        objectsRequest.setAll(all);
        objectsRequest.setUserUid(userUid);
        objectsRequest.setReplyToSuffix(replyToSuffix);
        objectsRequest.setParentId(parentId);
        return getObjects(sub, objectsRequest);
    }

    private JSTreeResponse getObject(Subscription sub, GetObjectRequest request) throws InterruptedException {

        log.trace("Requesting: {}", request);
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


        if (statusResponse.getObjectCount() == 0) {
            return null;
        }
        for (; ; ) {
            Message nextMsg = sub.nextMessage(RESPONSE_TIMEOUT);
            if (nextMsg != null) {
                String messageString = new String(nextMsg.getData(), StandardCharsets.UTF_8);
                JSTreeResponse jsTreeResponse = StaticMapper.parseObject(messageString, JSTreeResponse.class);
                if (jsTreeResponse != null) {
                    log.trace("Response: {}", jsTreeResponse);
                    return jsTreeResponse;
                }
            }
        }
    }

    private List<JSTreeResponse> getObjects(Subscription sub, GetObjectsRequest request) throws InterruptedException {

        log.trace("Requesting objects: {}", request);
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
        if (statusResponse.getObjectCount() == 0) {
            return result;
        }
        for (; ; ) {
            Message nextMsg = sub.nextMessage(RESPONSE_TIMEOUT);
            if (result.size() == statusResponse.getObjectCount()){
                List<JSTreeResponse> list = result.stream().filter(Objects::nonNull).toList();
                log.trace("Objects received: {}", list.size());
                return list;
            }
            if (nextMsg != null) {
                String messageString = new String(nextMsg.getData(), StandardCharsets.UTF_8);
                JSTreeResponse jsTreeResponse = StaticMapper.parseObject(messageString, JSTreeResponse.class);
                result.add(jsTreeResponse);
            }
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

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class WellState {
        private Long wellId;
        private String state;
    }
}
