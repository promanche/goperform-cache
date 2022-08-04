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
    public final int HISTORY_ONETIME_REQUESTS;

    @NotNull
    @Positive
    public final int RECONNECT_TIMEOUT_SECONDS;

    @NotNull
    public final List<Integer> SCALE_MINUTES;

    public final String HISTORY_NUID;

    public Config(@Value("${subject}") String subject,
                  @Value("${host}") String host,
                  @Value("${realtime-threads}") int realtimeThreads,
                  @Value("${history-threads}") int historyThreads,
                  @Value("${history-request-limit}") int historyRequestLimit,
                  @Value("${batch-size}") int batchSize,
                  @Value("${margin-size}") int marginSize,
                  @Value("${credentials-file}") String credentialsFile,
                  @Value("${history-onetime-requests}") int historyOnetimeRequests,
                  @Value("${reconnect-timeout-sec}") int reconnectTimeoutSeconds,
                  @Value("#{${scale-minutes}}") List<Integer> scaleMinutes) {
        this.SUBJECT = subject;
        this.HOST = host;
        this.REALTIME_THREADS = realtimeThreads;
        this.HISTORY_THREADS = historyThreads;
        this.HISTORY_REQUEST_LIMIT = historyRequestLimit;
        this.BATCH_SIZE = batchSize;
        this.MARGIN_SIZE = marginSize;
        this.CREDENTIALS_FILE = credentialsFile;
        this.HISTORY_ONETIME_REQUESTS = historyOnetimeRequests;
        this.RECONNECT_TIMEOUT_SECONDS = reconnectTimeoutSeconds;
        this.SCALE_MINUTES = scaleMinutes;

        this.HISTORY_NUID = NUID.nextGlobal();
    }
}
