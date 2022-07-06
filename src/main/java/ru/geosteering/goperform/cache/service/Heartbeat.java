package ru.geosteering.goperform.cache.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.storage.Storage;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class Heartbeat {

    private final Storage storage;

    @Scheduled(fixedDelay = 30, timeUnit = TimeUnit.SECONDS)
    private void heartbeat() {
        log.info(storage.getInfo());
    }
}
