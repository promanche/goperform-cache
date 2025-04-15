package ru.geosteering.goperform.cache.processor;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.metrics.MetricName;
import ru.geosteering.goperform.cache.metrics.MetricService;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Сборщик статистики для кривых.
 * <p>
 * Отслеживает состояние загрузки кривых, общее количество точек данных,
 * а также метрики для различных состояний (например, UNKNOWN, IN_QUEUE, LOADED).
 * <p>
 * Использует потокобезопасные коллекции и атомарные операции для обеспечения корректной работы в многопоточной среде.
 */
@Slf4j
@Component
@AllArgsConstructor
public class StatisticCollector {

    /**
     * Множество активных кривых.
     */
    private final Set<Long> activeCurves = ConcurrentHashMap.newKeySet();

    /**
     * Счетчик исторических точек.
     */
    private final AtomicInteger histCount = new AtomicInteger();

    /**
     * Счетчик точек реального времени.
     */
    private final AtomicInteger realCount = new AtomicInteger();

    /**
     * Сервис для работы с метриками.
     */
    private final MetricService metricService;

    /**
     * Собирает статистику о состоянии кривых и их точек данных.
     *
     * @param processors   Процессоры кривых.
     * @param brokenCurves Сломанные кривых.
     */
    public void collect(Map<Long, SingleCurveProcessor> processors, Map<Long, LocalDateTime> brokenCurves) {
        try {
            long timer = System.currentTimeMillis();
            // Сбор статистики по состояниям кривых
            Map<String, Integer> curvesInfo = processors.values().stream()
                    .map(SingleCurveProcessor::getLoadStatus)
                    .collect(Collectors.toMap(Enum::name, ls -> 1, Integer::sum));

            // Общее количество точек данных
            Long totalPoints = processors.values().stream()
                    .mapToLong(SingleCurveProcessor::totalBufferSize)
                    .sum();

            // Обновление метрик
            metricService.setGaugeValue(MetricName.CURVES_IN_TOTAL, processors.size());
            metricService.setGaugeValue(MetricName.TOTAL_POINTS, totalPoints.intValue());

            updateMetric(curvesInfo, MetricName.UNKNOWN, SingleCurveProcessor.LoadStatus.UNKNOWN);
            updateMetric(curvesInfo, MetricName.IN_QUEUE, SingleCurveProcessor.LoadStatus.IN_QUEUE);
            updateMetric(curvesInfo, MetricName.IN_PROGRESS, SingleCurveProcessor.LoadStatus.IN_PROGRESS);
            updateMetric(curvesInfo, MetricName.BLOCKED, SingleCurveProcessor.LoadStatus.BLOCKED);
            updateMetric(curvesInfo, MetricName.LOADED, SingleCurveProcessor.LoadStatus.LOADED);

            // Добавление дополнительных метрик
            curvesInfo.put("ACTIVE", activeCurves.size());
            metricService.setGaugeValue(MetricName.ACTIVE_CURVES, activeCurves.size());
            curvesInfo.put("BROKEN", brokenCurves.size());
            metricService.setGaugeValue(MetricName.BROKEN_CURVES, brokenCurves.size());

            log.info("DispatcherState.Curves: {}, total {} curves with {} points.", curvesInfo, processors.size(), totalPoints);

            // Расчет времени и скорости обработки данных
            long seconds = (System.currentTimeMillis() - timer) / 1000;
            seconds = seconds == 0 ? 1 : seconds;

            int history = histCount.getAndSet(0);
            metricService.setGaugeValue(MetricName.HISTORY_POINTS, history);
            int real = realCount.getAndSet(0);
            metricService.setGaugeValue(MetricName.REAL_POINTS, real);

            log.info("DispatcherState.Statistics: histPoints - {}, histPoints/sec - {}, real points - {}",
                    history, history / seconds, real);

            // Логирование кривых в очереди
            log.info("DispatcherState.Curves.InQueue: {}", processors.values().stream()
                    .filter(a -> a.getLoadStatus() == SingleCurveProcessor.LoadStatus.IN_QUEUE)
                    .map(a -> a.getInfo().getId())
                    .toList());
        } catch (Exception e) {
            log.error("Error while collecting statistics: {}", e.getMessage(), e);
        }
    }

    /**
     * Обрабатывает точку кривой.
     *
     * @param message Точка кривой.
     * @param isReal  Флаг, указывающий, является ли точка реального времени.
     */
    public void onCurveDataMessage(CurveDataMessage message, boolean isReal) {
        Long id = message.getId();
        if (isReal) {
            activeCurves.add(id);
            realCount.incrementAndGet();
            metricService.incrementGauge(MetricName.REAL_POINTS);
        } else {
            activeCurves.add(id);
            histCount.incrementAndGet();
            metricService.incrementGauge(MetricName.HISTORY_POINTS);
        }
    }

    /**
     * Обновляет метрику на основе информации о состоянии кривых.
     *
     * @param curvesInfo Мапа с информацией о состояниях кривых.
     * @param metricName Название метрики.
     * @param status     Статус загрузки кривой.
     */
    private void updateMetric(Map<String, Integer> curvesInfo, MetricName metricName, SingleCurveProcessor.LoadStatus status) {
        Integer count = Optional.ofNullable(curvesInfo.get(status.name())).orElse(0);
        metricService.setGaugeValue(metricName, count);
    }
}