package ru.geosteering.goperform.cache.temp;

import org.springframework.messaging.simp.stomp.*;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.util.concurrent.ExecutionException;

public class WebSocketClient {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        StandardWebSocketClient client = new StandardWebSocketClient();
        WebSocketStompClient stompClient = new WebSocketStompClient(client);

        ListenableFuture<StompSession> connect = stompClient.connect("ws://185.87.49.93:8080/ws", new StompSessionHandler() {
            @Override
            public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                session.subscribe("/websocket/CurveDataLoaded", this);
                System.out.println("Connected");
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
                System.out.println(exception.getMessage());

            }

            @Override
            public Type getPayloadType(StompHeaders headers) {
                return String.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                System.out.println((String) payload);
            }
        });

        while (connect.get().isConnected()) {

        }
    }
}
