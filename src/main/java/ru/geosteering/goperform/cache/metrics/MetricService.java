package ru.geosteering.goperform.cache.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class MetricService {

    private final MeterRegistry meterRegistry;
    private final Map<MetricName, AtomicInteger> gauges = new ConcurrentHashMap<>();


    public void incrementGauge(MetricName gaugeName) {
        AtomicInteger gauge = gauges.computeIfAbsent(gaugeName, name -> {
            AtomicInteger newGauge = new AtomicInteger(0);
            Gauge.builder(gaugeName.getName(), newGauge, AtomicInteger::get)
                    .description("Gauge for " + gaugeName.getName())
                    .register(meterRegistry);
            return newGauge;
        });
        gauge.incrementAndGet();
    }

    public void decrementGauge(MetricName gaugeName) {
        AtomicInteger gauge = gauges.computeIfAbsent(gaugeName, name -> {
            AtomicInteger newGauge = new AtomicInteger(0);
            Gauge.builder(gaugeName.getName(), newGauge, AtomicInteger::get)
                    .description("Gauge for " + gaugeName.getName())
                    .register(meterRegistry);
            return newGauge;
        });
        gauge.decrementAndGet();
    }

    public void setGaugeValue(MetricName gaugeName, int value) {
        AtomicInteger gauge = gauges.computeIfAbsent(gaugeName, name -> {
            AtomicInteger newGauge = new AtomicInteger(0);
            Gauge.builder(gaugeName.getName(), newGauge, AtomicInteger::get)
                    .description("Gauge for " + gaugeName.getName())
                    .register(meterRegistry);
            return newGauge;
        });
        gauge.set(value);
    }
}
