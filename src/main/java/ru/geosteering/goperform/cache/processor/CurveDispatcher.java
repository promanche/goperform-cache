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
import java.util.stream.Collectors;

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
    private final ScheduledExecutorService reloadExecutor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicInteger histCount = new AtomicInteger();
    private final AtomicInteger realCount = new AtomicInteger();
    private final Set<Long> activeCurves = ConcurrentHashMap.newKeySet();
    private long timer = System.currentTimeMillis();

    @PostConstruct
    private void runExecutors() {

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

        reloadExecutor.scheduleWithFixedDelay(() -> {
            try {
                processors.values().forEach(SingleCurveProcessor::reload);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 0, 5, TimeUnit.SECONDS);
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

        SingleCurveProcessor curveProcessor = getCurveProcessor(id, false);

        if (curveProcessor != null) {
            curveProcessor.onCurveDataMessage(message, isReal);
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

    public void removeFromRequestQueue(Long id) {
        synchronized (requestQueue) {
            requestQueue.removeIf(task -> Objects.equals(task.request.getCurveId(), id)
                    && (task.type == RequestType.LOAD_ACTIVE || task.type == RequestType.LOAD_REST));
        }
    }

    private CurveInfoResponse getCurveInfoResponse(Long id) {
        CurveInfoResponse response = null;

        SingleCurveProcessor curveProcessor = getCurveProcessor(id, true);

        if (curveProcessor != null) {
            ExtraCurveInfo info = curveProcessor.getInfo();

            response = new CurveInfoResponse(
                    info.getId(),
                    info.getMnemonic(),
                    info.getIndexType(),
                    info.getUnit(),
                    info.getAxisDefinition(),
                    info.getClassWitsml(),
                    info.getTypeLogData(),
                    info.getMaxValue(),
                    info.getMinValue(),
                    info.getMaxKey(),
                    info.getMinKey(),
                    curveProcessor.getSavedCount().get(),
                    curveProcessor.getScaleSet(),
                    info.getLastValue());
        }

        return response;
    }

    public boolean isBroken(Long id) {
        return brokenCurves.containsKey(id);
    }

    public void incrementHistCount(int count) {
        histCount.addAndGet(count);
    }

    public SingleCurveProcessor getCurveProcessor(Long id, boolean isRestRequest) {

        SingleCurveProcessor curveProcessor = processors.computeIfAbsent(id, key -> {

            ExtraCurveInfo info = repository.getInfo(id).orElse(null);
            if (info != null) {
                return new SingleCurveProcessor(info, this);
            }

            return null;
        });

        if (curveProcessor == null && notContainsRequest(id)) {
            CurveDataRequest request = new CurveDataRequest();
            request.setCurveId(id);
            request.setInfoOnly(true);
            request.setWithRange(true);
            RequestType type = isRestRequest ? RequestType.INFO_REST : RequestType.INFO_ACTIVE;
            requestQueue.add(new RequestTask(request, type));
        }

        return curveProcessor;
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

    private boolean notContainsRequest(Long id) {
        synchronized (requestQueue) {
            for (RequestTask task : requestQueue) {
                if (Objects.equals(task.request.getCurveId(), id)) {
                    return false;
                }
            }
            return true;
        }
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
                                ExtraCurveInfo info = new ExtraCurveInfo(curveInfo);
                                repository.saveOrUpdateInfo(info);
                                processors.computeIfAbsent(curveInfo.getId(), k -> new SingleCurveProcessor(info, this));
                            }
                            break;

                        case STATUS:

                            StatusMessage statusMessage = (StatusMessage) apiMessage;
                            log.error("Missing curveInfo for {}, message {}", requestTask.request.getCurveId(), statusMessage);
                            brokenCurves.computeIfAbsent(requestTask.request.getCurveId(), k -> LocalDateTime.now());
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
