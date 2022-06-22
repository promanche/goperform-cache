package ru.geosteering.goperform.cache.temp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SpeedTestMapper {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        Integer limit = 200000;
        AtomicInteger count = new AtomicInteger(0);
        String nuid = NUID.nextGlobal();
        int threads = 4;
        long curveId = 68708L;

        AtomicBoolean isEnd = new AtomicBoolean(false);

        ExecutorService service = Executors.newFixedThreadPool(threads);

        Map<Integer, ArrayList<Long>> map = new ConcurrentHashMap<>();

        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();

        PriorityBlockingQueue<CurveDataItem> queue = new PriorityBlockingQueue<>(100000, Comparator.comparing((CurveDataItem o) -> o.time));

        Options options = new Options.Builder()
                .connectionName("goperform-cache")
                .connectionListener((connection, events) -> System.out.println("Nats connection status: {}" + connection.getStatus()))
                .noReconnect()
                .authHandler(Nats.credentials("app.creds"))
                .server("tls://nats.geosteering.ru:4222")
                .build();

        Connection connection = Nats.connect(options);

        Dispatcher dispatcher = connection.createDispatcher();

        dispatcher.subscribe("gostream.curves" + "." + nuid + ".msgExample", msg -> {
            String json = new String(msg.getData());
            System.out.println("Message data example: " + json + ", bytes: " + msg.getData().length);
            System.out.println(Arrays.toString(msg.getData()));
        });

        dispatcher.subscribe("gostream.curves" + "." + nuid + ".test", msg ->
                service.submit(() -> {
                            int i = count.incrementAndGet();
                            ArrayList<Long> msgList = new ArrayList<>(4);
                            msgList.add(System.nanoTime());
                            String json = new String(msg.getData());
                            msgList.add(System.nanoTime());
                            CurveDataMessage curveDataMessage;
                            try {
                                curveDataMessage = mapper.readValue(json, CurveDataMessage.class);
                            } catch (JsonProcessingException e) {
                                isEnd.set(true);
                                System.err.println(e.getMessage());
                                System.out.println(new String(msg.getData()));
                                return;
                            }
                            msgList.add(System.nanoTime());
                            queue.add(curveDataMessage.data);
                            msgList.add(System.nanoTime());
                            map.put(i, msgList);
                        }
                ));


        CurveDataRequest msgExampleReq =
                new CurveDataRequest(curveId, null, null, null, false, false, 1, nuid + ".msgExample");
        Message msgExample = NatsMessage.builder()
                .subject("gostream.curves")
                .data(mapper.writeValueAsBytes(msgExampleReq))
                .build();
        connection.request(msgExample);


        CurveDataRequest request =
                new CurveDataRequest(curveId, null, null, null, false, false, limit, nuid + ".test");

        Message message = NatsMessage.builder()
                .subject("gostream.curves")
                .data(mapper.writeValueAsBytes(request))
                .build();

        System.out.println("request");
        CompletableFuture<Message> future = connection.request(message);
        Message response = future.get();
        long responseTime = System.nanoTime();
        System.out.println("response");

        while (!isEnd.get()) {
            System.out.println("received " + count.get());
            Thread.sleep(500);
        }
        long all = System.nanoTime();

        System.out.println("received " + count.get());
        System.out.println("time breakpoints " + map.values().stream().mapToInt(ArrayList::size).sum());

        ArrayList<Long> toString = new ArrayList<>();
        ArrayList<Long> toMessage = new ArrayList<>();
        ArrayList<Long> toQueue = new ArrayList<>();
        ArrayList<Long> full = new ArrayList<>();

        long firstMess = map.get(1).get(0);

        map.forEach((k, v) -> {
            Long in = v.get(0);
            Long str = v.get(1);
            Long mes = v.get(2);
            Long que = v.get(3);

            toString.add(str - in);
            toMessage.add(mes - str);
            toQueue.add(que - mes);
            full.add(que - in);
        });

        long toStringMin = toString.stream().mapToLong(Long::longValue).min().orElse(-1L);
        long toStringMax = toString.stream().mapToLong(Long::longValue).max().orElse(-1L);
        long toStringAver = (long) toString.stream().mapToLong(Long::longValue).average().orElse(-1L);

        long toMessageMin = toMessage.stream().mapToLong(Long::longValue).min().orElse(-1L);
        long toMessageMax = toMessage.stream().mapToLong(Long::longValue).max().orElse(-1L);
        long toMessageAver = (long) toMessage.stream().mapToLong(Long::longValue).average().orElse(-1L);

        long toQueueMin = toQueue.stream().mapToLong(Long::longValue).min().orElse(-1L);
        long toQueueMax = toQueue.stream().mapToLong(Long::longValue).max().orElse(-1L);
        long toQueueAver = (long) toQueue.stream().mapToLong(Long::longValue).average().orElse(-1L);

        long fullMin = full.stream().mapToLong(Long::longValue).min().orElse(-1L);
        long fullMax = full.stream().mapToLong(Long::longValue).max().orElse(-1L);
        long fullAver = (long) full.stream().mapToLong(Long::longValue).average().orElse(-1L);

        System.out.println("=============================STATISTIC (nanoseconds)=============================");
        System.out.println("Threads " + threads);
        System.out.println("Total messages " + queue.size());
        System.out.println("Total time " + (all - firstMess));
        System.out.println("response -> first message: " + (firstMess - responseTime));
        System.out.println("byte[] -> String: min - " + toStringMin + ", max - " + toStringMax + ", average - " + toStringAver);
        System.out.println("String -> CurveDataMessage: min - " + toMessageMin + ", max - " + toMessageMax + ", average - " + toMessageAver);
        System.out.println("CurveDataMessage -> Queue: min - " + toQueueMin + ", max - " + toQueueMax + ", average - " + toQueueAver);
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Full message processing: min - " + fullMin + ", max - " + fullMax + ", average - " + fullAver);
        System.out.println("=================================================================================");
        System.out.println("");

        connection.close();
        System.exit(0);
    }

    public static class CurveDataMessage {
        String type;
        Long id;
        CurveDataItem data;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public CurveDataItem getData() {
            return data;
        }

        public void setData(CurveDataItem data) {
            this.data = data;
        }
    }

    public static class CurveDataItem {
        OffsetDateTime time;
        Double depth;
        Double value;

        public OffsetDateTime getTime() {
            return time;
        }

        public void setTime(OffsetDateTime time) {
            this.time = time;
        }

        public Double getDepth() {
            return depth;
        }

        public void setDepth(Double depth) {
            this.depth = depth;
        }

        public Double getValue() {
            return value;
        }

        public void setValue(Double value) {
            this.value = value;
        }
    }

    public static class CurveDataRequest {
        private Long curveId;
        private String from;
        private String to;
        private Integer ms;
        private boolean infoOnly;
        private boolean withRange;
        private Integer limit;
        private String replyToSuffix;

        public CurveDataRequest(Long curveId, String from, String to, Integer ms, boolean infoOnly, boolean withRange, Integer limit, String replyToSuffix) {
            this.curveId = curveId;
            this.from = from;
            this.to = to;
            this.ms = ms;
            this.infoOnly = infoOnly;
            this.withRange = withRange;
            this.limit = limit;
            this.replyToSuffix = replyToSuffix;
        }

        public CurveDataRequest() {
        }

        public Long getCurveId() {
            return curveId;
        }

        public void setCurveId(Long curveId) {
            this.curveId = curveId;
        }

        public String getFrom() {
            return from;
        }

        public void setFrom(String from) {
            this.from = from;
        }

        public String getTo() {
            return to;
        }

        public void setTo(String to) {
            this.to = to;
        }

        public Integer getMs() {
            return ms;
        }

        public void setMs(Integer ms) {
            this.ms = ms;
        }

        public boolean isInfoOnly() {
            return infoOnly;
        }

        public void setInfoOnly(boolean infoOnly) {
            this.infoOnly = infoOnly;
        }

        public boolean isWithRange() {
            return withRange;
        }

        public void setWithRange(boolean withRange) {
            this.withRange = withRange;
        }

        public Integer getLimit() {
            return limit;
        }

        public void setLimit(Integer limit) {
            this.limit = limit;
        }

        public String getReplyToSuffix() {
            return replyToSuffix;
        }

        public void setReplyToSuffix(String replyToSuffix) {
            this.replyToSuffix = replyToSuffix;
        }
    }
}
