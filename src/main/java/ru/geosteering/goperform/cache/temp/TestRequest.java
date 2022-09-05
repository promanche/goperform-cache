package ru.geosteering.goperform.cache.temp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import ru.geosteering.goperform.cache.model.auth.JwtRequest;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class TestRequest {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        ObjectMapper mapper = new ObjectMapper();
        String nuid = NUID.nextGlobal();

        Options options = new Options.Builder()
                .connectionListener((connection, events) -> System.out.println("Nats connection status: {}" + connection.getStatus()))
                .noReconnect()
                .authHandler(Nats.credentials("app.creds"))
                .server("tls://nats.geosteering.ru:4222")
                .build();

        Connection connection = Nats.connect(options);

//        Dispatcher dispatcher = connection.createDispatcher();
//
//        dispatcher.subscribe("gostream.curves" + "." + nuid + ".msgExample", msg -> {
//            String json = new String(msg.getData());
//            System.out.println("Message: " + json + ", bytes: " + msg.getData().length);
//            System.out.println(Arrays.toString(msg.getData()));
//        });

        //CurveDataRequest request = new CurveDataRequest(24896L, null, null, null, false, false, 1, nuid + ".msgExample");

        JwtRequest request = new JwtRequest("brakhimov", "YD38hd0Dcjs$");

        Message response = NatsMessage.builder()
                .subject("gostream.auth")
                .data(mapper.writeValueAsBytes(request))
                .build();

        System.out.println(new String(connection.request(response).get().getData()));
    }
}
