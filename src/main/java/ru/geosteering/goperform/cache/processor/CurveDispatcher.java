package ru.geosteering.goperform.cache.processor;

import io.nats.client.Message;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.nats.ConnectionEventListener;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class CurveDispatcher implements ConnectionEventListener {


    protected final Config config;
    protected final MainRepository repository;
    protected final WebSocketMessageProcessor webSocketMessageProcessor;

    private final Map<Long, SingleCurveProcessor> processors = new ConcurrentHashMap<>();
    private final Map<Long, LocalDateTime> brokenCurves = new ConcurrentHashMap<>();
    private final Queue<RequestTask> requestTaskQueue = new PriorityBlockingQueue<>(11, Comparator.comparing(RequestTask::getPriority));
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
                synchronized (processors) {
                    Map<String, Integer> curvesInfo = processors.values().stream()
                            .map(SingleCurveProcessor::getLoadStatus)
                            .collect(Collectors.toMap(Enum::name, ls -> 1, Integer::sum));
                    curvesInfo.put("ACTIVE", activeCurves.size());
                    curvesInfo.put("BROKEN", brokenCurves.size());
                    log.info("CURVES INFO: {}", curvesInfo);

                    long seconds = (System.currentTimeMillis() - timer) / 1000;
                    timer = System.currentTimeMillis();

                    int history = histCount.getAndSet(0);
                    int real = realCount.getAndSet(0);

                    seconds = seconds == 0 ? 1 : seconds;

                    log.info("STATISTICS FOR THE PERIOD: histPoints - {}, histPoints/sec - {}, histPoint/sec/req - {}, real points - {}",
                            history, history / seconds, history / (seconds * config.NATS_ONETIME_REQUESTS), real);
                }
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
        requestTaskQueue.clear();
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
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(300));
        if (id != null) {
            processors.get(id).onDataEndMessage(message);
        }
        requestAllowed.incrementAndGet();
    }

    public void addRequestTask(RequestTask task) {
        requestTaskQueue.add(task);
    }

    public void removeFromRequestQueue(Long id) {
        synchronized (requestTaskQueue) {
            requestTaskQueue.removeIf(task -> Objects.equals(task.id, id)
                    && (task.type == RequestType.LOAD_ACTIVE || task.type == RequestType.LOAD_REST));
        }
    }

    public boolean isBroken(Long id) {
        return brokenCurves.containsKey(id);
    }

    public void incrementHistCount(int count) {
        histCount.addAndGet(count);
    }

    public SingleCurveProcessor getCurveProcessor(Long id, boolean fromRest) {
        SingleCurveProcessor curveProcessor = processors.computeIfAbsent(id, key -> {
            ExtraCurveInfo info = repository.getInfo(id).orElse(null);
            if (info != null) {
                return new SingleCurveProcessor(info, fromRest, this);
            }
            return null;
        });

        if (curveProcessor != null) {
            curveProcessor.setFromRest(fromRest);
        }

        if (curveProcessor == null && notContainsRequest(id)) {
            CurveDataRequest request = new CurveDataRequest();
            request.setCurveId(id);
            request.setInfoOnly(true);
            request.setWithRange(true);
            RequestType type = fromRest ? RequestType.INFO_REST : RequestType.INFO_ACTIVE;
            requestTaskQueue.add(new RequestTask(id, type, () -> doInfoRequest(id, fromRest)));
        }

        return curveProcessor;
    }

    private void doInfoRequest(Long id, boolean fromRest) {
        CurveDataRequest request = new CurveDataRequest();
        request.setCurveId(id);
        request.setInfoOnly(true);
        request.setWithRange(true);
        log.info("Request: {}", request);
        Message response = NatsConnector.sendRequest(config.SUBJECT, StaticMapper.toBytes(request));

        if (response != null) {
            log.info("Response: {}", new String(response.getData()));

            ApiMessage apiMessage = StaticMapper.parseObject(new String(response.getData()), ApiMessage.class);

            switch (Objects.requireNonNull(apiMessage).getType()) {

                case CURVE_INFO:
                    CurveInfo curveInfo = ((CurveInfoMessage) apiMessage).getCurveInfo();
                    ExtraCurveInfo info = new ExtraCurveInfo(curveInfo);
                    repository.saveOrUpdateInfo(info);
                    processors.computeIfAbsent(curveInfo.getId(), k -> new SingleCurveProcessor(info, fromRest, this));
                    break;

                case STATUS:
                    StatusMessage statusMessage = (StatusMessage) apiMessage;
                    if (statusMessage.getStatus() != EResult.OK) {
                        log.error("Missing curveInfo for {}, message {}", id, statusMessage);
                        brokenCurves.computeIfAbsent(id, k -> LocalDateTime.now());
                    }
                    break;

                default:
                    log.error("Unknown response {}", new String(response.getData()));
                    break;
            }
        }

        requestAllowed.incrementAndGet();
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
        synchronized (requestTaskQueue) {
            for (RequestTask task : requestTaskQueue) {
                if (Objects.equals(task.id, id)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Scheduled(fixedRate = 30)
    private void doRequestJob() {
        if (NatsConnector.isConnected()) {
            try {
                if (requestAllowed.getAndDecrement() > 0 && !requestTaskQueue.isEmpty()) {
                    requestTaskQueue.poll().requestJob.doRequest();
                } else {
                    requestAllowed.incrementAndGet();
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    @Scheduled(fixedDelay = 3, timeUnit = TimeUnit.MINUTES)
    private void clearBroken() {
        synchronized (brokenCurves) {
            brokenCurves.entrySet().removeIf(entry -> entry.getValue().plusMinutes(30).isBefore(LocalDateTime.now()));
        }
    }

    protected interface RequestJob {
        void doRequest();
    }

    @AllArgsConstructor
    protected static class RequestTask {

        Long id;
        RequestType type;
        RequestJob requestJob;

        int getPriority() {
            return type.priority;
        }
    }

    protected enum RequestType {

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
