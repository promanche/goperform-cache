package ru.geosteering.goperform.cache.config;

import io.nats.client.NUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.*;
import java.util.List;

@Component
@ConfigurationProperties("config")
@Validated
public class Config {

    @NotBlank
    public final String SUBJECT;

    @NotBlank
    public final String HOST;

    @NotNull
    @Positive
    public final int REALTIME_THREADS;

    @NotNull
    @Positive
    public final int HISTORY_THREADS;

    @NotNull
    @Positive
    public final int HISTORY_REQUEST_LIMIT;

    @NotNull
    @Positive
    public final int BATCH_SIZE;

    @NotNull
    @Positive
    public final int MARGIN_SIZE;

    @NotBlank
    public final String CREDENTIALS_FILE;

    @NotNull
    @Positive
    public final int NATS_ONETIME_REQUESTS;

    @NotNull
    @Positive
    public final int RECONNECT_TIMEOUT_SECONDS;

    @NotNull
    public final List<Integer> SCALE_MINUTES;

    @Positive
    public final int STATISTIC_PERIOD_SECONDS;

    public final String HISTORY_NUID;

    public Config(@Value("${goperform" + '.' + "subject}") String subject,
                  @Value("${goperform.host}") String host,
                  @Value("${goperform.realtime-threads}") int realtimeThreads,
                  @Value("${goperform.history-threads}") int historyThreads,
                  @Value("${goperform.history-request-limit}") int historyRequestLimit,
                  @Value("${goperform.batch-size}") int batchSize,
                  @Value("${goperform.margin-size}") int marginSize,
                  @Value("${goperform.credentials-file}") String credentialsFile,
                  @Value("${goperform.nats-onetime-requests}") int natsOnetimeRequests,
                  @Value("${goperform.reconnect-timeout-sec:10}") int reconnectTimeoutSeconds,
                  @Value("#{${goperform.scale-minutes}}") List<Integer> scaleMinutes,
                  @Value("${goperform.statistic-period-sec:30}") int statisticPeriodSeconds) {
        this.SUBJECT = subject;
        this.HOST = host;
        this.REALTIME_THREADS = realtimeThreads;
        this.HISTORY_THREADS = historyThreads;
        this.HISTORY_REQUEST_LIMIT = historyRequestLimit;
        this.BATCH_SIZE = batchSize;
        this.MARGIN_SIZE = marginSize;
        this.CREDENTIALS_FILE = credentialsFile;
        this.NATS_ONETIME_REQUESTS = natsOnetimeRequests;
        this.RECONNECT_TIMEOUT_SECONDS = reconnectTimeoutSeconds;
        this.SCALE_MINUTES = scaleMinutes;
        this.STATISTIC_PERIOD_SECONDS = statisticPeriodSeconds;

        this.HISTORY_NUID = NUID.nextGlobal();
    }
}
