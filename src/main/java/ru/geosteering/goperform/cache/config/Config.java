package ru.geosteering.goperform.cache.config;

import io.nats.client.NUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Класс конфигурации приложения. Значения загружаются из файла application.properties.
 */
@Component
@ConfigurationProperties("config")
@Validated
public class Config {

    /**
     * Очередь NATS для получения данных по кривым
     */
    @NotBlank
    public final String SUBJECT;

    /**
     * Очередь NATS для получения древа объектов
     */
    @NotBlank
    public final String OBJECTS;

    /**
     * Очередь NATS для аутентификации
     */
    @NotBlank
    public final String AUTH;

    /**
     * Хост для подключения NATS
     */
    @NotBlank
    public final String HOST;

    /**
     * Логин пользователя NATS
     */
    @NotBlank
    public final String GOSTREAM_USERNAME;

    /**
     * Пароль пользователя NATS
     */
    @NotBlank
    public final String GOSTREAM_PASSWORD;




    /**
     * Количество потоков для обработки данных в реальном времени
     */
    @NotNull
    @Positive
    public final int REALTIME_THREADS;

    /**
     * Количество потоков для обработки исторических данных
     */
    @NotNull
    @Positive
    public final int HISTORY_THREADS;

    /**
     * Лимит по количеству точек в запросе истории
     */
    @NotNull
    @Positive
    public final int HISTORY_REQUEST_LIMIT;

    /**
     * Количество точек для сохранения данных в БД одной строкой
     */
    @NotNull
    @Positive
    public final int BATCH_SIZE;

    /**
     * Запас точек, чтобы перекрыть возможные нарушения хронологии при доставке точек через NATS.
     * Сохранение {@link #BATCH_SIZE} точек в БД происходит только при накоплении не менее {@link #BATCH_SIZE} + {@link #MARGIN_SIZE} точек
     */
    @NotNull
    @Positive
    public final int MARGIN_SIZE;

    /**
     * Путь до файла с кредами для доступа к NATS
     */
    @NotBlank
    public final String CREDENTIALS_FILE;

    /**
     * Максимальное количество одновременных запросов через NATS
     */
    @NotNull
    @Positive
    public final int NATS_ONETIME_REQUESTS;

    /**
     * Таймаут при повторных реконнектах NATS
     */
    @NotNull
    @Positive
    public final int RECONNECT_TIMEOUT_SECONDS;

    /**
     * Набор шкал для сегментации кривых (минут на единицу шкалы)
     */
    @NotNull
    public final List<Integer> SCALE_MINUTES;

    /**
     * Период вывода статистики в лог
     */
    @NotNull
    @Positive
    public final int STATISTIC_PERIOD_SECONDS;

    /**
     * NUID для запросов в NATS. Генерируется при старте приложения
     */
    public final String HISTORY_NUID;

    /**
     * Минимальное допустимое значение ключа для кривых по времени
     */
    public final long MIN_TIME_MILLIS;

    /**
     * Минимальное допустимое значение ключа для кривых по глубине
     */
    public final double MIN_DEPTH_METERS;

    /**
     * Максимальное допустимое значение ключа для кривых по глубине
     */
    public final double MAX_DEPTH_METERS;

    /**
     * URL для доступа к датасервису по АПИ. Сейчас не используется
     */
    @NotBlank
    public final String DATA_SERVICE_BASEURL;

    /**
     * Время в минутах для кэширования данных с помощью {@link org.springframework.cache.annotation.Cacheable}.
     * Сейчас кэширование используется при авторизации и аутентификации
     */
    @NotNull
    @Positive
    public final int CACHEABLE_DURATION_MINUTES;

    /**
     * Число дней после которого кривые должны быть удалены
     */
    public final int DAYS_UNTIL_CURVE_IS_REMOVED;

    @NotBlank
    @Positive
    public final int DAYS_UNTIL_CURVE_PROCESSOR_IS_REMOVED;




    public Config(@Value("${goperform" + '.' + "subject}") String subject,
                  @Value("${goperform" + '.' + "objects}") String objects,
                  @Value("${goperform" + '.' + "auth}") String auth,
                  @Value("${goperform.host}") String host,
                  @Value("${goperform.nats.username}") String username,
                  @Value("${goperform.nats.password}") String password,
                  @Value("${goperform.realtime-threads}") int realtimeThreads,
                  @Value("${goperform.history-threads}") int historyThreads,
                  @Value("${goperform.history-request-limit}") int historyRequestLimit,
                  @Value("${goperform.batch-size}") int batchSize,
                  @Value("${goperform.margin-size}") int marginSize,
                  @Value("${goperform.credentials-file}") String credentialsFile,
                  @Value("${goperform.nats-onetime-requests}") int natsOnetimeRequests,
                  @Value("${goperform.reconnect-timeout-sec:10}") int reconnectTimeoutSeconds,
                  @Value("#{${goperform.scale-minutes}}") List<Integer> scaleMinutes,
                  @Value("${goperform.statistic-period-sec:30}") int statisticPeriodSeconds,
                  @Value("${goperform.dataservice-baseurl}") String dataServiceBaseUrl,
                  @Value("${goperform.cacheable-duration-min:10}") int cacheableDurationMin,
                  @Value("${goperform.cleaning.curve.days}") int daysUntilCurveIsRemoved,
                  @Value("${goperform.cleaning.processor.days}") int daysUntilCurveProcessorIsRemoved) {
        this.SUBJECT = subject;
        this.OBJECTS = objects;
        this.AUTH = auth;
        this.HOST = host;
        this.GOSTREAM_USERNAME = username;
        this.GOSTREAM_PASSWORD = password;
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
        this.MIN_TIME_MILLIS = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant().toEpochMilli();
        this.MIN_DEPTH_METERS = 0;
        this.MAX_DEPTH_METERS = 13000;

        this.DATA_SERVICE_BASEURL = dataServiceBaseUrl;
        this.CACHEABLE_DURATION_MINUTES = cacheableDurationMin;

        this.DAYS_UNTIL_CURVE_IS_REMOVED = daysUntilCurveIsRemoved;
        this.DAYS_UNTIL_CURVE_PROCESSOR_IS_REMOVED = daysUntilCurveProcessorIsRemoved;
    }
}
