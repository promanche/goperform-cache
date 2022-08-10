package ru.geosteering.goperform.cache.temp;

import org.springframework.messaging.simp.stomp.*;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class WebSocketClient {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        StandardWebSocketClient client = new StandardWebSocketClient();
        WebSocketStompClient stompClient = new WebSocketStompClient(client);

        WebSocketHttpHeaders headers = new WebSocketHttpHeaders();
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJicmFraGltb3YiLCJhdXRob3JpdGllcyI6WyJST0xFX1VTRVIiLCJST0xFX1dNTFMiXSwiaXNzIjoiaHR0cDovL2dlb3N0ZWVyaW5nLnJ1IiwiaWF0IjoxNjU5OTc2NTk0fQ.frkQ_xNS2C5otmQsFb5KBKGd5o0ezIskKPS1QJx6PK0WYnzZFLX5FCpmjobYqDYvFuDmnHHaFzRE-iJoSlyOZg";
        headers.put("Authorization", List.of("Bearer " + token));

        ListenableFuture<StompSession> connect = stompClient.connect("ws://localhost:8080/ws", headers, new StompSessionHandler() {
            @Override
            public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                System.out.println("Connected");
                session.subscribe("/curve/55/loaded", this);
                session.subscribe("/curve/55/newPoint", this);
            }

            @Override
            public void handleException(StompSession session, StompCommand command, StompHeaders headers, byte[] payload, Throwable exception) {
                System.out.println("HANDLE EXCEPTION");
                System.out.println(command);
                System.out.println(headers);
                System.out.println(new String(payload));
                System.out.println(exception.getMessage());
                System.out.println("-----------------------------------");
            }

            @Override
            public void handleTransportError(StompSession session, Throwable exception) {
                System.out.println("HANDLE TransportError");
                System.out.println(exception);

            }

            @Override
            public Type getPayloadType(StompHeaders headers) {
                return String.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                System.out.println("HANDLE Frame");
                System.out.println(headers.getContentType());
                System.out.println(headers.getContentType());
                System.out.println(headers);
                System.out.println((String) payload);
            }
        });

        while (connect.get().isConnected()) {

        }
    }
}
