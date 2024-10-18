package ru.geosteering.goperform.cache.processor;

import io.nats.client.Message;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.EResult;
import ru.geosteering.commonModels.dataService.CurveInfo;
import ru.geosteering.commonModels.dataService.requests.CurveDataRequest;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.exception.NullResponseException;
import ru.geosteering.goperform.cache.metrics.MetricName;
import ru.geosteering.goperform.cache.metrics.MetricService;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.nats.ConnectionEventListener;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.PerformCacheState;
import ru.geosteering.goperform.cache.service.ApiServiceRestClientService;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Диспетчер обработчиков кривых. Хранит {@linkplain #processors карту} с {@linkplain SingleCurveProcessor обработчиками кривых}.
 *
 * <p> При получении точки кривой или rest запроса передает данные в соответствующий обработчик.
 * При отсутствии нужного обработчика создает запрос на получение информации по кривой и создает обработчик.
 *
 * <p> Содержит {@linkplain #requestQueue очередь задач} для запросов в NATS и следит, чтобы одновременно выполнялось не более {@link Config#NATS_ONETIME_REQUESTS} запросов.
 * См. {@link RequestTask} и {@link #doRequestJob()}. Запросы асинхронные с приоритетом.
 *
 * <p> Также содержит scheduled сервисы для вывода статистики и перезагрузки кривых
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class CurveDispatcher implements ConnectionEventListener {


    protected final Config config;
    protected final MainRepository repository;
    protected final WebSocketMessageProcessor webSocketMessageProcessor;
    private final ApiServiceRestClientService apiServiceRestClientService;

    private final Map<Long, SingleCurveProcessor> processors = new ConcurrentHashMap<>();
    private final Map<Long, LocalDateTime> brokenCurves = new ConcurrentHashMap<>();
    private final SynchronizedRequestQueue requestQueue = new SynchronizedRequestQueue();
    private final AtomicInteger requestAllowed = new AtomicInteger();
    private final ScheduledExecutorService statExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService reloadExecutor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicInteger histCount = new AtomicInteger();
    private final AtomicInteger realCount = new AtomicInteger();
    private final Set<Long> activeCurves = ConcurrentHashMap.newKeySet();
    private long timer = System.currentTimeMillis();
    private final ConcurrentMap<Long, LocalDateTime> curvesLastChange = new ConcurrentHashMap<>();

    private final MetricService metricService;

    @PostConstruct
    private void runExecutors() {

        statExecutor.scheduleWithFixedDelay(() -> {
            try {
                Map<String, Integer> curvesInfo = processors.values().stream()
                        .map(SingleCurveProcessor::getLoadStatus)
                        .collect(Collectors.toMap(Enum::name, ls -> 1, Integer::sum));
                Long totalPoints = processors.values().stream().collect(Collectors.summingLong(SingleCurveProcessor::totalBufferSize));
                metricService.setGaugeValue(MetricName.CURVES_IN_TOTAL, processors.size());
                metricService.setGaugeValue(MetricName.TOTAL_POINTS, totalPoints.intValue());

                Integer unknownStatus = Optional.ofNullable(curvesInfo.get(SingleCurveProcessor.LoadStatus.UNKNOWN.name())).orElse(0);
                metricService.setGaugeValue(MetricName.UNKNOWN, unknownStatus);
                Integer inQueueStatus = Optional.ofNullable(curvesInfo.get(SingleCurveProcessor.LoadStatus.IN_QUEUE.name())).orElse(0);
                metricService.setGaugeValue(MetricName.IN_QUEUE, inQueueStatus);
                Integer inProgressStatus = Optional.ofNullable(curvesInfo.get(SingleCurveProcessor.LoadStatus.IN_PROGRESS.name())).orElse(0);
                metricService.setGaugeValue(MetricName.IN_PROGRESS, inProgressStatus);
                Integer blockedStatus = Optional.ofNullable(curvesInfo.get(SingleCurveProcessor.LoadStatus.BLOCKED.name())).orElse(0);
                metricService.setGaugeValue(MetricName.BLOCKED, blockedStatus);
                Integer loadedStatus = Optional.ofNullable(curvesInfo.get(SingleCurveProcessor.LoadStatus.LOADED.name())).orElse(0);
                metricService.setGaugeValue(MetricName.LOADED, loadedStatus);

                curvesInfo.put("ACTIVE", activeCurves.size());
                metricService.setGaugeValue(MetricName.ACTIVE_CURVES, activeCurves.size());
                curvesInfo.put("BROKEN", brokenCurves.size());
                curvesInfo.put("REQUEST_ALLOWED", requestAllowed.get());
                log.info("DispatcherState.Curves: {}, total {} curves with {} points.", curvesInfo, processors.size(), totalPoints);

                long seconds = (System.currentTimeMillis() - timer) / 1000;
                timer = System.currentTimeMillis();

                int history = histCount.getAndSet(0);
                metricService.setGaugeValue(MetricName.HISTORY_POINTS, history);
                int real = realCount.getAndSet(0);
                metricService.setGaugeValue(MetricName.REAL_POINTS, real);

                seconds = seconds == 0 ? 1 : seconds;

                log.info("DispatcherState.Statistics: histPoints - {}, histPoints/sec - {}, real points - {}",
                        history, history / seconds, real);

                log.info("DispatcherState.Curves.InQueue: {}", processors.values().stream().filter(a -> a.getLoadStatus() == SingleCurveProcessor.LoadStatus.IN_QUEUE).map(a -> a.getInfo().getId()).toList());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 0, config.STATISTIC_PERIOD_SECONDS, TimeUnit.SECONDS);

        reloadExecutor.scheduleWithFixedDelay(() -> {
            try {
                processors.values().forEach(SingleCurveProcessor::reload);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 0, 5, TimeUnit.SECONDS);

    }


    /**
     * Определение времени последнего обновления для каждой кривой.
     */

    @EventListener(ApplicationStartedEvent.class)
    public void initCurvesLastChange() {
        List<Long> infoIds = repository.getInfoIds();
        log.info("There are {} curves stored in cache", infoIds.size());

        setCurvesActualState(infoIds);
    }

    private void setCurvesStoredState(List<Long> infoIds) {
        List<PerformCacheState> performCacheStates = repository.getAllStates();
        performCacheStates.forEach(state -> {
            if (infoIds.contains(state.getId()) && state.getWellId() != null) {
                curvesLastChange.put(state.getId(), state.getUpdatedAt().toLocalDateTime());
            }
        });
    }

    private void setCurvesActualState(List<Long> infoIds) {
        setCurvesStoredState(infoIds);

        Set<Long> savedCurvesWithState = curvesLastChange.keySet();
        infoIds.removeAll(savedCurvesWithState);

        if (!infoIds.isEmpty()) {
            Map<ApiServiceRestClientService.WellState, List<Long>> allWellsCurves = apiServiceRestClientService.getAllWellsCurves(infoIds);
            infoIds.forEach(id -> {

                LocalDateTime lastChange = curvesLastChange.get(id);
                for (Map.Entry<ApiServiceRestClientService.WellState, List<Long>> entry : allWellsCurves.entrySet()) {
                    if (entry.getValue().contains(id)) {
                        if (lastChange == null) {
                            switch (entry.getKey().getState()) {
                                case "WELL_GREEN" -> lastChange = LocalDateTime.now();
                                case "WELL_YELLOW" -> lastChange = LocalDateTime.now().minusMinutes(10);
                                case "WELL_RED" -> lastChange = LocalDateTime.now().minusDays(1);
                                case "WELL" -> lastChange = LocalDateTime.now().minusDays(30);
                            }
                            curvesLastChange.put(id, lastChange);
                        }
                        log.debug("Curve {} last change was {}", id, lastChange);
                        repository.saveOrUpdateState(
                                new PerformCacheState(id, lastChange.atOffset(ZoneOffset.UTC), entry.getKey().getWellId().toString()));
                    }
                }
            });
        }
    }

    @Override
    public void onConnect() {
        requestAllowed.set(config.NATS_ONETIME_REQUESTS);
        metricService.setGaugeValue(MetricName.REQUEST_ALLOWED, config.NATS_ONETIME_REQUESTS);
        processors.values().forEach(SingleCurveProcessor::onConnect);
    }

    @Override
    public void onDisconnect() {
        requestAllowed.set(0);
        metricService.setGaugeValue(MetricName.REQUEST_ALLOWED, 0);
        processors.values().forEach(SingleCurveProcessor::onDisconnect);
        requestQueue.clear();
    }

    public void onCurveDataMessage(CurveDataMessage message, boolean isReal) {
        Long id = message.getId();
        if (isReal) {
            activeCurves.add(id);
            realCount.incrementAndGet();
            metricService.incrementGauge(MetricName.REAL_POINTS);
        }
        if (isBroken(id)) {
            return;
        }
        SingleCurveProcessor curveProcessor = getCurveProcessor(id, false);
        if (curveProcessor != null) {
            curveProcessor.onCurveDataMessage(message, isReal);
        }
    }

    public void onDataEndMessage(DataEndMessage message, String subject) {
        try {
            Long id = parseId(subject);
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(300));
            if (id != null) {
                getCurveProcessor(id, false).onDataEndMessage(message);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            requestAllowed.incrementAndGet();
            metricService.incrementGauge(MetricName.REQUEST_ALLOWED);
        }
    }

    protected void addRequestTask(RequestTask newTask) {
        log.debug("Adding request task {} for {}", newTask.type, newTask.id);
        requestQueue.add(newTask);
    }

    private void removeLoadTask(Long id) {
        removeLoadTask(id, false);
    }

    protected void removeLoadTask(Long id, boolean skipLogging) {
        if (!skipLogging) {
            log.debug("Removing load task for {} (if any)", id);
        }
        requestQueue.removeIf(task -> Objects.equals(task.id, id)
                && (task.type == RequestType.LOAD_ACTIVE || task.type == RequestType.LOAD_REST));
    }

    public boolean isBroken(Long id) {
        return brokenCurves.containsKey(id);
    }

    protected void incrementHistCount(int count) {
        histCount.addAndGet(count);
    }

    public SingleCurveProcessor getCurveProcessor(Long id, boolean fromRest) {
        SingleCurveProcessor curveProcessor = processors.computeIfAbsent(id, key -> {
            ExtraCurveInfo info = repository.getInfo(id).orElse(null);
            if (info != null) {
                metricService.incrementGauge(MetricName.CURVES_IN_TOTAL);
                return new SingleCurveProcessor(info, fromRest, this);
            }
            return null;
        });
        if (curveProcessor != null) {
            curveProcessor.setFromRest(fromRest);
        } else {
            RequestType type = fromRest ? RequestType.INFO_REST : RequestType.INFO_ACTIVE;
            addRequestTask(new RequestTask(id, type, () -> doInfoRequest(id, fromRest)));
        }
        curvesLastChange.put(id, LocalDateTime.now());

        return curveProcessor;
    }

    private void doInfoRequest(Long id, boolean fromRest) {
        try {
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
                    case CURVE_INFO -> {
                        CurveInfo curveInfo = ((CurveInfoMessage) apiMessage).getCurveInfo();
                        ExtraCurveInfo info = new ExtraCurveInfo(prepareInfoKeys(curveInfo));
                        repository.saveOrUpdateInfo(info);
                        processors.computeIfAbsent(curveInfo.getId(), k -> {
                            metricService.incrementGauge(MetricName.CURVES_IN_TOTAL);
                            return new SingleCurveProcessor(info, fromRest, this);
                        });
                    }
                    case STATUS -> {
                        StatusMessage statusMessage = (StatusMessage) apiMessage;
                        if (statusMessage.getStatus() != EResult.OK) {
                            log.error("Missing curveInfo for {}, message {}", id, statusMessage);
                            brokenCurves.computeIfAbsent(id, k -> {
                                metricService.incrementGauge(MetricName.BROKEN_CURVES);
                                return LocalDateTime.now();
                            });
                        }
                    }
                    default -> log.error("Unknown response {}", new String(response.getData()));
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            requestAllowed.incrementAndGet();
            metricService.incrementGauge(MetricName.REQUEST_ALLOWED);
        }
    }

    private CurveInfo prepareInfoKeys(CurveInfo info) {

        Double mdMin = info.getMdMin();
        Double mdMax = info.getMdMax();
        if (mdMin != null && (mdMin < config.MIN_DEPTH_METERS || mdMin > config.MAX_DEPTH_METERS)) {
            info.setMdMin(null);
        }
        if (mdMax != null && (mdMax < config.MIN_DEPTH_METERS || mdMax > config.MAX_DEPTH_METERS)) {
            info.setMdMax(null);
        }

        OffsetDateTime timeMin = info.getTimeMin();
        OffsetDateTime timeMax = info.getTimeMax();
        if (timeMin != null && (timeMin.toInstant().toEpochMilli() < config.MIN_TIME_MILLIS || timeMin.toInstant().toEpochMilli() > OffsetDateTime.now().plusHours(24).toInstant().toEpochMilli())) {
            info.setTimeMin(null);
        }
        if (timeMax != null && (timeMax.toInstant().toEpochMilli() < config.MIN_TIME_MILLIS || timeMax.toInstant().toEpochMilli() > OffsetDateTime.now().plusHours(24).toInstant().toEpochMilli())) {
            info.setTimeMax(null);
        }

        return info;
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

    @Scheduled(fixedRate = 30)
    private void doRequestJob() {
        if (NatsConnector.isConnected()) {
            if (requestAllowed.getAndDecrement() > 0 && !requestQueue.isEmpty()) {
                try {
                    metricService.decrementGauge(MetricName.REQUEST_ALLOWED);
                    requestQueue.poll().requestJob.doRequest();
                } catch (NullResponseException e) {
                    metricService.incrementGauge(MetricName.REQUEST_ALLOWED);
                    requestAllowed.incrementAndGet();
                }
            } else {
                requestAllowed.incrementAndGet();
            }
        }
    }

    @Scheduled(fixedDelay = 3, timeUnit = TimeUnit.MINUTES)
    private void clearBroken() {
        try {
            AtomicInteger count = new AtomicInteger();
            brokenCurves.entrySet().removeIf(entry -> {
                boolean removable = entry.getValue().plusMinutes(30).isBefore(LocalDateTime.now());
                if (removable) {
                    count.getAndIncrement();
                    metricService.decrementGauge(MetricName.BROKEN_CURVES);
                }
                return removable;
            });
            if (count.get() > 0) {
                log.info("{} curves removed from broken", count);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected void onErrorDataRequest(Long id) {
        if (!brokenCurves.containsKey(id)) {
            LocalDateTime now = LocalDateTime.now();
            brokenCurves.put(id, now);
            metricService.incrementGauge(MetricName.BROKEN_CURVES);
            log.warn("Curve {} is marked as broken at {} for 30 minutes", id, now);
        } else {
            log.debug("Curve {} is already marked as broken", id);
        }

        SingleCurveProcessor removed = processors.remove(id);
        if (removed != null) {
            log.warn("Curve {} processor was removed", id);
        } else {
            log.debug("Curve {} processor is null", id);
        }

        requestAllowed.incrementAndGet();
        metricService.incrementGauge(MetricName.REQUEST_ALLOWED);
    }

    /**
     * Сброс кэша кривой по id
     *
     * @param id кривой
     */

    public void fullCurveReload(Long id) {
        curvesLastChange.put(id, LocalDateTime.now());
        processors.compute(id, (k, v) -> {
            removeLoadTask(id);
            deleteCurveById(id);
            addRequestTask(new RequestTask(id, RequestType.INFO_REST, () -> doInfoRequest(id, true)));
            return null;
        });
    }

    /**
     * Удаление кривой по id
     *
     * @param id кривой
     */

    public void deleteCurve(Long id) {
        processors.compute(id, (k, v) -> {
            deleteCurveById(id);
            return null;
        });
        log.debug("Curve {} was removed", id);
    }

    private void deleteCurveById(Long id) {
        repository.deleteInfo(id);

        boolean isSegmentsExist = repository.getSegmentsRecordsCount(id) > 0;
        boolean isItemsExist = repository.getItemsRecordsCount(id) > 0;
        while (isSegmentsExist || isItemsExist) {
            if (isItemsExist) {
                isItemsExist = repository.deleteBatchItems(id);
            }
            if (isSegmentsExist) {
                isSegmentsExist = repository.deleteBatchSegments(id);
            }
        }
    }

    /**
     * Удаление кривых и их обработчиков
     */
    @Scheduled(fixedDelay = 12, initialDelay = 3, timeUnit = TimeUnit.HOURS)
    private void deleteInactiveCurves() {
        List<Long> deletedCurves = new ArrayList<>();

        curvesLastChange.forEach((id, lastChange) -> {
            if (lastChange.isBefore(LocalDateTime.now().minusDays(config.DAYS_UNTIL_CURVE_PROCESSOR_IS_REMOVED))) {
                removeLoadTask(id);
                processors.remove(id);
                log.debug("SingleCurveProcessor was removed for curve {}", id);
            }
            if (lastChange.isBefore(LocalDateTime.now().minusDays(config.DAYS_UNTIL_CURVE_IS_REMOVED))) {
                deleteCurveById(id);
                deletedCurves.add(id);
            }
        });
        if (!deletedCurves.isEmpty()) {
            deletedCurves.forEach(curvesLastChange::remove);
        }
        log.debug("{} curves were removed because they were inactive", deletedCurves.size());
    }

    /**
     * Обновление времени последнего обновления кривых
     */
    @Scheduled(fixedDelay = 3, initialDelay = 1, timeUnit = TimeUnit.HOURS)
    private void updateState() {
        curvesLastChange.forEach((id, lastChange) -> {
                    if (lastChange.isAfter(LocalDateTime.now().minusDays(1))) {
                        PerformCacheState state = new PerformCacheState(id, lastChange.atOffset(ZoneOffset.UTC), null);
                        repository.saveOrUpdateState(state);
                    }
                }
        );
        log.debug("State values were updated");
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

    private static class SynchronizedRequestQueue {

        private final PriorityQueue<RequestTask> queue;

        public SynchronizedRequestQueue() {
            this.queue = new PriorityQueue<>(Comparator.comparing(RequestTask::getPriority));
        }

        public synchronized void add(RequestTask newTask) {
            boolean contains = false;
            for (RequestTask task : queue) {
                if (Objects.equals(task.id, newTask.id) && task.type == newTask.type) {
                    contains = true;
                    break;
                }
            }
            if (!contains) {
                queue.add(newTask);
            }
        }

        public synchronized void clear() {
            queue.clear();
        }

        public synchronized void removeIf(Predicate<? super RequestTask> filter) {
            queue.removeIf(filter);
        }

        public synchronized RequestTask poll() {
            return queue.poll();
        }

        public synchronized boolean isEmpty() {
            return queue.isEmpty();
        }
    }
}
