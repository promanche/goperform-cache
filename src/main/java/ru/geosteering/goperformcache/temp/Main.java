//package ru.geosteering.goperformcache;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.SerializationFeature;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import io.nats.client.*;
//import lombok.AllArgsConstructor;
//import lombok.Getter;
//import lombok.Setter;
//import org.apache.avro.Schema;
//import org.apache.avro.io.Decoder;
//import org.apache.avro.io.DecoderFactory;
//import org.apache.avro.reflect.ReflectDatumReader;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.atomic.AtomicLong;
//
//public class Main {
//    private static String TOPIC = "gostream.curves";
//    private static final String REPLY_TO_PREFIX = "test";
//    private static Integer LIMIT = 100000;
//    private static final ConcurrentHashMap<Long, Stat> stat = new ConcurrentHashMap<>();
//    private static Dispatcher dispatcher;
//    private static final ObjectMapper mapper = new ObjectMapper();
//    private static final Schema dataEndMessageSchema;
//    private static final Schema apiMessageSchema;
//    private static final HashMap<Long,LinkedBlockingQueue<byte[]>> queues = new HashMap<>();
//
//    private static final String API_MESSAGE_SCHEMA = "{\n" +
//            "  \"type\" : \"record\",\n" +
//            "  \"name\" : \"ApiMessage\",\n" +
//            "  \"namespace\" : \"ru.geosteering.models\",\n" +
//            "  \"fields\" : [{\n" +
//            "    \"name\" : \"type\",\n" +
//            "    \"type\" : {\n" +
//            "      \"type\" : \"enum\",\n" +
//            "      \"name\" : \"MessageType\",\n" +
//            "      \"namespace\" : \"ru.geosteering.models.ApiMessage\",\n" +
//            "      \"symbols\" : [ \"CURVE_DATA\", \"DATA_END\", \"CURVE_INFO\", \"WELL_INFO\", \"STATUS\" ]\n" +
//            "    }\n" +
//            "  }]\n" +
//            "}";
//
//    private static final String DATA_END_MESSAGE_SCHEMA = "{\n" +
//            "  \"type\" : \"record\",\n" +
//            "  \"name\" : \"DataEndMessage\",\n" +
//            "  \"namespace\" : \"ru.geosteering.models\",\n" +
//            "  \"fields\" : [{\n" +
//            "    \"name\" : \"type\",\n" +
//            "    \"type\" : {\n" +
//            "      \"type\" : \"enum\",\n" +
//            "      \"name\" : \"MessageType\",\n" +
//            "      \"namespace\" : \"ru.geosteering.models.ApiMessage\",\n" +
//            "      \"symbols\" : [ \"CURVE_DATA\", \"DATA_END\", \"CURVE_INFO\", \"WELL_INFO\", \"STATUS\" ]\n" +
//            "    }\n" +
//            "  }, {\n" +
//            "    \"name\" : \"sentCount\",\n" +
//            "    \"type\" : \"int\"\n" +
//            "  }]\n" +
//            "}";
//
//    private static final String DATA_MESSAGE_SCHEMA = "{\n" +
//            "  \"type\" : \"record\",\n" +
//            "  \"name\" : \"CurveDataMessage\",\n" +
//            "  \"namespace\" : \"ru.geosteering.models\",\n" +
//            "  \"fields\" : [{\n" +
//            "    \"name\" : \"type\",\n" +
//            "    \"type\" : {\n" +
//            "      \"type\" : \"enum\",\n" +
//            "      \"name\" : \"MessageType\",\n" +
//            "      \"namespace\" : \"ru.geosteering.models.ApiMessage\",\n" +
//            "      \"symbols\" : [ \"CURVE_DATA\", \"DATA_END\", \"CURVE_INFO\", \"WELL_INFO\", \"STATUS\" ]\n" +
//            "    }\n" +
//            "  }, {\n" +
//            "    \"name\" : \"data\",\n" +
//            "    \"type\" : [ \"null\", {\n" +
//            "      \"type\" : \"record\",\n" +
//            "      \"name\" : \"CurveDataItem\",\n" +
//            "      \"namespace\" : \"ru.geosteering.models\",\n" +
//            "      \"fields\" : [ {\n" +
//            "        \"name\" : \"depth\",\n" +
//            "        \"type\" : [ \"null\", {\n" +
//            "          \"type\" : \"double\",\n" +
//            "          \"java-class\" : \"java.lang.Double\"\n" +
//            "        } ],\n" +
//            "        \"default\" : null\n" +
//            "      }, {\n" +
//            "        \"name\" : \"time\",\n" +
//            "        \"type\" : [ \"null\", {\n" +
//            "          \"type\" : \"long\",\n" +
//            "          \"logicalType\" : \"timestamp-millis\",\n" +
//            "          \"CustomEncoding\" : \"OffsetDateTimeAsLongEncoding\"\n" +
//            "        } ],\n" +
//            "        \"default\" : null\n" +
//            "      }, {\n" +
//            "        \"name\" : \"value\",\n" +
//            "        \"type\" : [ \"null\", {\n" +
//            "          \"type\" : \"double\",\n" +
//            "          \"java-class\" : \"java.lang.Double\"\n" +
//            "        } ],\n" +
//            "        \"default\" : null\n" +
//            "      } ]\n" +
//            "    } ]\n" +
//            "  }, {\n" +
//            "    \"name\" : \"id\",\n" +
//            "    \"type\" : {\n" +
//            "      \"type\" : \"long\",\n" +
//            "      \"java-class\" : \"java.lang.Long\"\n" +
//            "    }\n" +
//            "  } ]\n" +
//            "}";
//
//    private static long[] curves;
//    private static final long[] curvesDemo = new long[]{
//        24233,
//        24234,
//        24235,
//        24236,
//        24237,
//        24238,
//        24239,
//        24240,
//        24241,
//        24242
//    };
//    private static final long[] curvesOnline = new long[]{
//        94815,
//        94816,
//        94817,
//        94818,
//        94819,
//        94820,
//        94821,
//        94822,
//        94823,
//        94824
//    };
//
//    private static final long[] curvesLocal = new long[]{
//        23844,
//        23845,
//        23846,
//        23847,
//        23848,
//        23849,
//        23850,
//        23851,
//        23852,
//        23853
//    };
//
//    private static final long[] curvesLukonline = new long[]{
//        33340,
//        33341,
//        33342,
//        33343,
//        33344,
//        33345,
//        33346,
//        33347,
//        33348,
//        33349
//    };
//
//    private static enum Servers {
//        LOCAL,
//        ONLINE,
//        DEMO,
//        LUKONLINE
//    }
//
//    static class Stat {
//        HashMap<String, Integer> cnt = new HashMap<>();
//        long size;
//        long start;
//        long end;
//        long avParseDuration;
//        int sentCnt;
//        boolean work = true;
//    }
//
//    @Getter
//    @Setter
//    @AllArgsConstructor
//    static class MessageWarpper {
//        Long cid;
//        Message message;
//    }
//
//    static {
//        try {
//            mapper.registerModule(new JavaTimeModule());
//            mapper.disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE);
//            mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
//            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//            dataEndMessageSchema = new Schema.Parser().setValidate(true).parse(DATA_END_MESSAGE_SCHEMA);
//            apiMessageSchema = new Schema.Parser().setValidate(true).parse(API_MESSAGE_SCHEMA);
//
//        } catch (Exception e) {
//            System.out.println("Exception in Avro mapper init");
//            throw new RuntimeException(e);
//        }
//    }
//
//    public static void main(String[] args) {
//        int curvesCnt = 0;
//        Servers server = Servers.DEMO;
//        String serverURL = null;
//        AtomicBoolean binary = new AtomicBoolean(false);
//        for(String a : args) {
//            if (a.startsWith("-limit=")) {
//                try { LIMIT = Integer.parseInt(a.replace( "-limit=", "" )); }
//                catch( NumberFormatException e ){
//                    System.out.printf("Failed to parse limit(%s) - using default(100000)%n", a.replace( "-limit=", "" ));
//                }
//            }
//            if (a.startsWith("-curves=")) {
//                try {
//                    curvesCnt = Integer.parseInt(a.replace( "-curves=", "" ));
//                } catch( NumberFormatException e ){
//                    System.out.printf("Failed to parse curves count(%s) - using default(10)%n", a.replace( "-curves=", "" ));
//                }
//            }
//            if (a.startsWith("-server=")) {
//                if ("online".equalsIgnoreCase(a.replace( "-server=", "" )))
//                    server = Servers.ONLINE;
//                else if ("local".equalsIgnoreCase(a.replace( "-server=", "" )))
//                    server = Servers.LOCAL;
//                else if ("lukoil".equalsIgnoreCase(a.replace( "-server=", "" )))
//                    server = Servers.LUKONLINE;
//                else
//                    server = Servers.DEMO;
//            }
//            if (a.startsWith("-serverUrl=")) {
//                serverURL = a.replace( "-serverUrl=", "" );
//            }
//            if (a.startsWith("-binary")) {
//                binary.set(true);
//            }
//        }
//        switch (server) {
//            case LOCAL:
//                if (curvesCnt < 0 || curvesCnt > curvesLocal.length) {
//                    System.out.printf("Curves count(%d) out of range(0-%d) or isn't defined - use default(%d)%n",
//                            curvesCnt, curvesLocal.length, curvesLocal.length);
//                    curvesCnt = curvesLocal.length;
//                }
//                curves = curvesCnt == 0 ? new long[] {0} : Arrays.copyOfRange(curvesLocal, 0, curvesCnt);
//                if (serverURL == null) serverURL = "nats://localhost:4222";
//                TOPIC = "curves";
//                break;
//
//            case ONLINE:
//                if (curvesCnt < 0 || curvesCnt > curvesOnline.length) {
//                    System.out.printf("Curves count(%d) out of range(0-%d) or isn't defined - use default(%d)%n",
//                            curvesCnt, curvesOnline.length, curvesOnline.length);
//                    curvesCnt = curvesOnline.length;
//                }
//                curves = curvesCnt == 0 ? new long[] {0} : Arrays.copyOfRange(curvesOnline, 0, curvesCnt);
//                if (serverURL == null) serverURL = "nats://ONLINE:4222";
//                break;
//
//            case DEMO:
//                if (curvesCnt < 0 || curvesCnt > curvesDemo.length) {
//                    System.out.printf("Curves count(%d) out of range(0-%d) or isn't defined - use default(%d)%n",
//                            curvesCnt, curvesDemo.length, curvesDemo.length);
//                    curvesCnt = curvesDemo.length;
//                }
//                curves = curvesCnt == 0 ? new long[] {0} : Arrays.copyOfRange(curvesDemo, 0, curvesCnt);
//                if (serverURL == null) serverURL = "tls://nats.geosteering.ru:4222";
//                break;
//
//            case LUKONLINE:
//                if (curvesCnt < 0 || curvesCnt > curvesLukonline.length) {
//                    System.out.printf("Curves count(%d) out of range(0-%d) or isn't defined - use default(%d)%n",
//                            curvesCnt, curvesLukonline.length, curvesLukonline.length);
//                    curvesCnt = curvesLukonline.length;
//                }
//                curves = curvesCnt == 0 ? new long[] {0} : Arrays.copyOfRange(curvesLukonline, 0, curvesCnt);
//                if (serverURL == null) serverURL = "nats://LUKONLINE:4222";
//                break;
//        }
//        System.out.println("Test.start");
//        System.out.printf("Requesting %d curves with a limit of %d points each from server(%s)%n", curves.length, LIMIT, serverURL);
//        if (curvesCnt == 0)
//            System.out.println("* Using generated test curve without DB access");
//        try {
//            AuthHandler authHandler = Nats.credentials("app.creds");
//            Options opt = new Options.Builder()
//                                        .authHandler(authHandler)
//                                        .errorListener(new MyErrorListener())
//                                        .server(serverURL)
//                                        .build();
//            Connection nc = Nats.connect(opt);
//            dispatcher = nc.createDispatcher(messageHandler());
//            dispatcher.setPendingLimits(200000, 64 * 1024 * 1024);
//            ExecutorService executor = Executors.newFixedThreadPool(curves.length);
//            AtomicLong totalSize = new AtomicLong(0);
//            AtomicLong totalDuration = new AtomicLong(0);
//            HashMap<Long, CompletableFuture<MessageWarpper>> responses = new HashMap<>();
//            for(long cid : curves) {
//                queues.put(cid, new LinkedBlockingQueue<>());
//                dispatcher.subscribe(TOPIC + "." + REPLY_TO_PREFIX + cid);
//                Stat st = new Stat();
//                st.start = System.nanoTime();
//                stat.put(cid, st);
//                CurveDataRequest req = new CurveDataRequest(cid, null, null, false, false, LIMIT, REPLY_TO_PREFIX + cid, binary.get());
//                System.out.printf("[%d]Request: %s%n", cid, mapper.writeValueAsString(req));
//                CompletableFuture<Message> respMsg = nc.request(TOPIC, mapper.writeValueAsBytes(req));
//                CompletableFuture<MessageWarpper> resp = respMsg.thenApply(msg -> new MessageWarpper(cid, msg));
//                responses.put(cid, resp);
//
//                executor.submit(() -> {
//                    try {
//                        LinkedBlockingQueue<byte[]> queue = queues.get(cid);
//                        do {
//                            byte[] messageData = queue.poll(60, TimeUnit.SECONDS);
//                            long parseStart = System.nanoTime();
//                            ApiMessage response = null;
//                            if (messageData != null){
//                                ReflectDatumReader<ApiMessage> reader = new ReflectDatumReader<>(apiMessageSchema);
//                                Decoder decoder = DecoderFactory.get().binaryDecoder(messageData, null);
//                                response = binary.get() ? reader.read(null, decoder)
//                                                        : mapper.readValue(messageData, ApiMessage.class);
//                            }
//                            long parseDuration = System.nanoTime() - parseStart;
//                            if (response != null) {
//                                int cnt = st.cnt.values().stream().mapToInt(Integer::intValue).sum();
//                                st.cnt.putIfAbsent(response.getType().toString(), 0);
//                                st.cnt.put(response.getType().toString(), st.cnt.get(response.getType().toString()) + 1);
//                                st.size += messageData.length;
//                                st.avParseDuration = (st.avParseDuration * cnt + parseDuration) / (cnt + 1);
//                                if (response.getType() == ApiMessage.MessageType.DATA_END) {
//                                    if (binary.get()){
//                                        ReflectDatumReader<DataEndMessage> reader = new ReflectDatumReader<>(dataEndMessageSchema);
//                                        Decoder decoder = DecoderFactory.get().binaryDecoder(messageData, null);
//                                        response = reader.read(null, decoder);
//                                    }
//                                    st.work = false;
//                                    st.end = System.nanoTime();
//                                    st.sentCnt = ((DataEndMessage) response).getSentCount();
//                                }
//                            } else {
//                                st.work = false;
//                                st.end = System.nanoTime();
//                            }
//                        } while (st.work);
//                        long duration = (st.end - st.start) / 1000000;
//                        totalSize.addAndGet(st.size);
//                        totalDuration.addAndGet(duration);
//                        dispatcher.unsubscribe(TOPIC + "." + REPLY_TO_PREFIX + cid);
//                        StringBuilder sb = new StringBuilder();
//                        for(String key : st.cnt.keySet())
//                            sb.append(String.format("[%d]Got packets[%s]: %d\r\n", cid, key, st.cnt.get(key)));
//                        if (st.size != 0)
//                            sb.append(String.format("[%d]Sent count match:%s; Size: %.01fMb(%db) in %d ms; avParseTime: %.01f mks",
//                                    cid, st.sentCnt == st.cnt.get(ApiMessage.MessageType.CURVE_DATA.toString()),
//                                    st.size/(1024.0*1024.0), st.size, duration, st.avParseDuration / 1000.0));
//                        else
//                            sb.append(String.format("[%d]No data received", cid));
//                        System.out.println(sb.toString());
//
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                });
//            }
//
//            while(!responses.isEmpty()) {
//                CompletableFuture<Object> result = CompletableFuture.anyOf(responses.values().toArray(new CompletableFuture[0]));
//                MessageWarpper msgWrapper = (MessageWarpper) result.get();
//                System.out.printf("[%d]Response: %s%n", msgWrapper.getCid(), new String(msgWrapper.getMessage().getData(), StandardCharsets.UTF_8));
//                responses.remove(msgWrapper.getCid());
//            }
//            executor.shutdown();
//            executor.awaitTermination(5, TimeUnit.MINUTES);
//            if (!executor.isTerminated()) {
//                List<Runnable> tasks = executor.shutdownNow();
//                System.out.println("Forced shutdown. Tasks awaiting: " + tasks.size());
//            } else
//                System.out.println("Shutdown gracefully");
//            nc.close();
//            long duration = totalDuration.get() / curves.length;
//            double speed = (totalSize.get() * 1000.0) / (1024.0 * 1024.0 * duration);
//            System.out.printf("Total data received: %.01fMb(%db) in %d ms; Speed: %.01fMbyte/s (%.01fMbit/s)%n",
//                    totalSize.get()/(1024.0*1024.0), totalSize.get(), duration, speed, speed * 8.38);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("Test.end");
//    }
//
//    private static MessageHandler messageHandler() {
//        return new MessageHandler() {
//
//            @Override
//            public void onMessage(Message msg) throws InterruptedException {
//                try {
//                    long curveId = Long.parseLong(msg.getSubject().substring(TOPIC.length() + REPLY_TO_PREFIX.length() + 1));
//                    //System.out.printf("OnMessage[%d]: %s\r\n", curveId, Arrays.toString(msg.getData()));
//                    queues.get(curveId).add(msg.getData());
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//    }
//
//    static class MyErrorListener implements ErrorListener {
//        @Override
//        public void slowConsumerDetected(Connection conn, Consumer consumer) {
//            System.out.println("NATS connection slow consumer detected");
//        }
//
//        @Override
//        public void exceptionOccurred(Connection conn, Exception exp) {
//            System.out.println("NATS connection exception occurred");
//            exp.printStackTrace();
//        }
//
//        @Override
//        public void errorOccurred(Connection conn, String error) {
//            System.out.println("NATS connection error occurred " + error);
//        }
//    }
//
//}
