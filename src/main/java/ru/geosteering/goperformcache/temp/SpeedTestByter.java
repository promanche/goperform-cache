package ru.geosteering.goperformcache.temp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import lombok.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SpeedTestByter {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        Integer limit = 200000;
        AtomicInteger count = new AtomicInteger(0);
        String nuid = NUID.nextGlobal();
        int threads = 4;
        Long curveId = 68708L;

        AtomicBoolean isEnd = new AtomicBoolean(false);

        ExecutorService service = Executors.newFixedThreadPool(threads);

        Map<Integer, ArrayList<Long>> map = new ConcurrentHashMap<>();

        Byter byter = new Byter();

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
                            ArrayList<Long> msgList = new ArrayList<>(3);
                            msgList.add(System.nanoTime());
                            CurveDataMessage curveDataMessage = byter.get(msg.getData());
                            if (curveDataMessage == null) {
                                isEnd.set(true);
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

        ArrayList<Long> toMessage = new ArrayList<>();
        ArrayList<Long> toQueue = new ArrayList<>();
        ArrayList<Long> full = new ArrayList<>();

        long firstMess = map.get(1).get(0);

        map.forEach((k, v) -> {
            Long in = v.get(0);
            Long mes = v.get(1);
            Long que = v.get(2);

            toMessage.add(mes - in);
            toQueue.add(que - mes);
            full.add(que - in);
        });

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
        System.out.println("byte[] -> CurveDataMessage: min - " + toMessageMin + ", max - " + toMessageMax + ", average - " + toMessageAver);
        System.out.println("CurveDataMessage -> Queue: min - " + toQueueMin + ", max - " + toQueueMax + ", average - " + toQueueAver);
        System.out.println("---------------------------------------------------------------------------------");
        System.out.println("Full message processing: min - " + fullMin + ", max - " + fullMax + ", average - " + fullAver);
        System.out.println("=================================================================================");
        System.out.println("");

        connection.close();
        System.exit(0);
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class CurveDataMessage {
        String type;
        String id;
        CurveDataItem data;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class CurveDataItem {
        LocalDateTime time;
        String depth;
        String value;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CurveDataRequest {
        private Long curveId;
        private String from;
        private String to;
        private Integer ms;
        private boolean infoOnly;
        private boolean withRange;
        private Integer limit;
        private String replyToSuffix;
    }

    public static class Byter {

        public CurveDataMessage get(byte[] bytes) {

            try {
                String id = new String(bytes, 19, 5);
                int year = Integer.parseInt(new String(bytes, 41, 4));
                int month = Integer.parseInt(new String(bytes, 46, 2));
                int day = Integer.parseInt(new String(bytes, 49, 2));
                int hour = Integer.parseInt(new String(bytes, 52, 2));
                int minute = Integer.parseInt(new String(bytes, 55, 2));
                int second = Integer.parseInt(new String(bytes, 58, 2));
                String depth = new String(bytes, 71, 3);
                String value = new String(bytes, 83, bytes.length - 85);

                CurveDataItem item = new CurveDataItem(LocalDateTime.of(year, month, day, hour, minute, second), depth, value);

                return new CurveDataMessage(null, id, item);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return null;
            }
        }
    }
}
