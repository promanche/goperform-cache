package ru.geosteering.goperform.cache.config;

import io.nats.client.NUID;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.util.Properties;

@Slf4j
public class Config {

    public static final String SUBJECT;
    public static final String HOST;
    public static final int REALTIME_THREADS;
    public static final int HISTORY_THREADS;
    public static final int HISTORY_REQUEST_LIMIT;
    public static final int BATCH_SIZE;
    public static final int MARGIN_SIZE;
    public static final String CREDENTIALS_FILE;
    public static final String HISTORY_NUID;
    public static final int HISTORY_ONETIME_REQUESTS;
    public static final int RECONNECT_TIMEOUT_SECONDS;

    static {

        Properties properties = loadProperties();

        SUBJECT = properties.getProperty("subject", "gostream.curves");
        HOST = properties.getProperty("host", "tls://nats.geosteering.ru:4222");
        REALTIME_THREADS = Integer.parseInt(properties.getProperty("realtime-threads", "2"));
        HISTORY_THREADS = Integer.parseInt(properties.getProperty("history-threads", "6"));
        HISTORY_REQUEST_LIMIT = Integer.parseInt(properties.getProperty("history-request-limit", "25000"));
        BATCH_SIZE = Integer.parseInt(properties.getProperty("batch-size", "1000"));
        MARGIN_SIZE = Integer.parseInt(properties.getProperty("margin-size", "20"));
        CREDENTIALS_FILE = properties.getProperty("credentials-file", "app.creds");
        HISTORY_ONETIME_REQUESTS = Integer.parseInt(properties.getProperty("history-onetime-requests", "2"));
        RECONNECT_TIMEOUT_SECONDS = Integer.parseInt(properties.getProperty("reconnect-timeout-sec", "10"));

        HISTORY_NUID = NUID.nextGlobal();

    }

    private static Properties loadProperties() {
        Properties properties = new Properties();

        File file = new File("application.properties");
        if (file.exists() && file.isFile()) {
            try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                properties.load(reader);
            } catch (Exception e) {
                log.error("Reading properties from file exception: {}", e.getMessage(), e);
            }

        }

        return properties;
    }
}
