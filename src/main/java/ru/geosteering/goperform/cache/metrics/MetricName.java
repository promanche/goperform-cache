package ru.geosteering.goperform.cache.metrics;

import lombok.Getter;


@Getter
public enum MetricName {
    CURVES_IN_TOTAL("curves_in_total"),
    TOTAL_POINTS("total_points"),
    HISTORY_POINTS("history_points"),
    REAL_POINTS("real_points"),
    REQUEST_ALLOWED("request_allowed"),
    BROKEN_CURVES("broken_curves"),
    ACTIVE_CURVES("active_curves"),
    UNKNOWN("curve_status_unknown"),
    IN_QUEUE("curve_status_in_queue"),
    IN_PROGRESS("curve_status_in_progress"),
    LOADED("curve_status_loaded"),
    BLOCKED("curve_blocked");

    MetricName(String name) {
        this.name = name;
    }

    private final String name;
}
