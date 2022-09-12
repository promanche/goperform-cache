package ru.geosteering.goperform.cache.processor;

import io.nats.client.Message;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveSegment;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.model.rest.CurveInfoResponse;
import ru.geosteering.goperform.cache.nats.ConnectionEventListener;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Slf4j
@RequiredArgsConstructor
public class CurveDispatcher implements ConnectionEventListener {

    @Getter
    private final Config config;
    @Getter
    private final MainRepository repository;
    @Getter
    private final WebSocketMessageProcessor webSocketMessageProcessor;

    private final Map<Long, SingleCurveProcessor> processors = new ConcurrentHashMap<>();
    private final Map<Long, LocalDateTime> brokenCurves = new ConcurrentHashMap<>();
    private final Queue<RequestTask> requestQueue = new PriorityBlockingQueue<>(11, Comparator.comparing(RequestTask::getPriority));

    private final AtomicInteger requestAllowed = new AtomicInteger();

    private final ScheduledExecutorService statExecutor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicInteger histCount = new AtomicInteger();
    private final AtomicInteger realCount = new AtomicInteger();
    private final Set<Long> activeCurves = ConcurrentHashMap.newKeySet();
    private long timer = System.currentTimeMillis();

    @PostConstruct
    private void runStatExecutor() {
        statExecutor.scheduleAtFixedRate(() -> {
            try {
                Map<String, Integer> curvesInfo = processors.values().stream()
                        .map(SingleCurveProcessor::getLoadStatus)
                        .collect(Collectors.toMap(ls -> ls.get().name(), ls -> 1, Integer::sum));
                curvesInfo.put("ACTIVE", activeCurves.size());
                curvesInfo.put("BROKEN", brokenCurves.size());
                log.info("CURVES INFO: {}", curvesInfo);

                long seconds = (System.currentTimeMillis() - timer) / 1000;
                timer = System.currentTimeMillis();

                int history = histCount.getAndSet(0);
                int real = realCount.getAndSet(0);

                log.info("STATISTICS FOR THE PERIOD: histPoints - {}, histPoints/sec - {}, histPoint/sec/req - {}, real points - {}",
                        history, history / seconds, history / (seconds * config.NATS_ONETIME_REQUESTS), real);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, config.STATISTIC_PERIOD_SECONDS, config.STATISTIC_PERIOD_SECONDS, TimeUnit.SECONDS);
    }


    @Override
    public void onConnect() {
        requestAllowed.set(config.NATS_ONETIME_REQUESTS);
        processors.values().forEach(SingleCurveProcessor::onConnect);
    }

    @Override
    public void onDisconnect() {
        requestAllowed.set(0);
        requestQueue.clear();
        processors.values().forEach(SingleCurveProcessor::onDisconnect);
    }

    public void onCurveDataMessage(CurveDataMessage message, boolean isReal) {

        Long id = message.getId();

        if (isReal) {
            activeCurves.add(id);
            realCount.incrementAndGet();
        }

        if (brokenCurves.containsKey(id)) {
            return;
        }

        if (processors.containsKey(id)) {
            processors.get(id).onCurveDataMessage(message, isReal);

        } else if (isReal) {
            Optional<ExtraCurveInfo> optional = repository.getInfo(id);

            if (optional.isPresent()) {
                processors.computeIfAbsent(id, k -> new SingleCurveProcessor(optional.get(), this))
                        .onCurveDataMessage(message, true);

            } else {
                requestQueue.add(new RequestTask(createInfoRequest(id), RequestType.INFO_ACTIVE));
            }
        }
    }

    public void onDataEndMessage(DataEndMessage message, String subject) {

        Long id = parseId(subject);

        if (id != null) {
            processors.get(id).onDataEndMessage(message);
        }

        requestAllowed.incrementAndGet();
    }


    public void addRequestTask(RequestTask task) {
        requestQueue.add(task);
    }

    public List<?> getCurveData(Long id, Double from, Double to, Integer scale) {

        if (processors.containsKey(id)) {
            SingleCurveProcessor curveProcessor = processors.get(id);

            curveProcessor.setHaveRestRequest(true);

            if (scale != null && curveProcessor.getScaleSet().contains(scale)) {
                return repository.getSegmentsFromTo(id, scale, from, to)
                        .stream()
                        .flatMap((Function<String, Stream<CurveSegment>>) str -> StaticMapper.parseListOf(str, CurveSegment.class).stream())
                        .collect(Collectors.toList());

            } else {
                return curveProcessor.getCurveData(from, to);
            }
        }

        requestQueue.add(new RequestTask(createInfoRequest(id), RequestType.INFO_REST));
        return null;
    }


    public boolean reload(Long id, Double from) {
        if (processors.containsKey(id)) {

            processors.get(id).updateReloadData(from, 0);

            synchronized (requestQueue) {
                requestQueue.removeIf(task -> Objects.equals(task.request.getCurveId(), id)
                        && (task.type == RequestType.LOAD_ACTIVE || task.type == RequestType.LOAD_REST));
            }
            return true;
        }

        return false;
    }

    public ExtraCurveInfo getCurveInfo(Long id) {
        if (processors.containsKey(id)) {
            return processors.get(id).getInfo();
        }

        requestQueue.add(new RequestTask(createInfoRequest(id), RequestType.INFO_REST));
        return null;
    }

    public CurveInfoResponse getCurveInfoResponse(Long id) {
        if (processors.containsKey(id)) {
            SingleCurveProcessor curveProcessor = processors.get(id);
            ExtraCurveInfo info = curveProcessor.getInfo();

            return new CurveInfoResponse(
                    info.getId(),
                    info.getMnemonic(),
                    info.getIndexType(),
                    info.getUnit(),
                    info.getAxisDefinition(),
                    info.getClassWitsml(),
                    info.getTypeLogData(),
                    info.getMaxValue(),
                    info.getMinValue(),
                    curveProcessor.getMaxKey(),
                    curveProcessor.getMinKey(),
                    curveProcessor.getSavedCount(),
                    curveProcessor.getScaleSet(),
                    curveProcessor.getLastValue()
            );
        }

        requestQueue.add(new RequestTask(createInfoRequest(id), RequestType.INFO_REST));
        return null;
    }

    public boolean isBroken(Long id) {
        return brokenCurves.containsKey(id);
    }

    public void incrementHistCount(int count) {
        histCount.addAndGet(count);
    }

    private Long parseId(String subject) {

        try {
            String[] arr = subject.split("\\.");
            return Long.parseLong(arr[arr.length - 1]);
        } catch (Exception e) {
            log.error("Parsing id from subject exception: {}", subject, e);
            return null;
        }
    }

    private CurveDataRequest createInfoRequest(Long id) {

        CurveDataRequest request = new CurveDataRequest();
        request.setCurveId(id);
        request.setInfoOnly(true);
        request.setWithRange(true);

        return request;
    }

    @Scheduled(fixedRate = 30)
    private void sendRequest() {
        try {
            if (requestAllowed.getAndDecrement() > 0 && !requestQueue.isEmpty()) {
                RequestTask requestTask = requestQueue.poll();

                if (requestTask.type == RequestType.LOAD_ACTIVE || requestTask.type == RequestType.LOAD_REST) {
                    processors.get(requestTask.request.getCurveId()).setRequestTimer(System.currentTimeMillis());
                }

                Message response = NatsConnector.sendRequest(config.SUBJECT, StaticMapper.toBytes(requestTask.request));

                if (response != null) {
                    ApiMessage apiMessage = StaticMapper.parseObject(new String(response.getData()), ApiMessage.class);

                    switch (Objects.requireNonNull(apiMessage).getType()) {

                        case CURVE_INFO:

                            CurveInfo curveInfo = ((CurveInfoMessage) apiMessage).getCurveInfo();
                            if (requestTask.type == RequestType.INFO_REST || requestTask.type == RequestType.INFO_ACTIVE) {
                                processors.putIfAbsent(curveInfo.getId(), new SingleCurveProcessor(new ExtraCurveInfo(curveInfo, null, null), this));
                            }
                            break;

                        case STATUS:

                            StatusMessage statusMessage = (StatusMessage) apiMessage;
                            log.error("Missing curveInfo for {}, message {}", requestTask.request.getCurveId(), statusMessage);
                            brokenCurves.putIfAbsent(requestTask.request.getCurveId(), LocalDateTime.now());
                            break;

                        default:
                            log.error("Unknown response {}", new String(response.getData()));
                            break;
                    }

                    if (requestTask.type == RequestType.INFO_REST || requestTask.type == RequestType.INFO_ACTIVE) {
                        requestAllowed.incrementAndGet();
                    }
                }

            } else {
                requestAllowed.incrementAndGet();
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            requestAllowed.incrementAndGet();
        }
    }

    @Scheduled(fixedDelay = 3, timeUnit = TimeUnit.MINUTES)
    private void clearBroken() {
        synchronized (brokenCurves) {
            brokenCurves.entrySet().removeIf(entry -> entry.getValue().plusMinutes(30).isBefore(LocalDateTime.now()));
        }
    }

    public static class RequestTask {

        CurveDataRequest request;
        RequestType type;

        public RequestTask(CurveDataRequest request, RequestType type) {
            this.request = request;
            this.type = type;
        }

        int getPriority() {
            return type.priority;
        }
    }

    public enum RequestType {

        INFO_REST(0),
        INFO_ACTIVE(1),
        LOAD_REST(2),
        LOAD_ACTIVE(3);

        final int priority;

        RequestType(int priority) {
            this.priority = priority;
        }
    }
}
