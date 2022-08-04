package ru.geosteering.goperform.cache.service;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.storage.Storage;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class DataReloader {

    private final MainRepository repository;
    private final HistoryLoader loader;
    private final Storage storage;

    private final Map<Long, ReloadData> reloadData;

    public DataReloader(MainRepository repository, HistoryLoader loader, Storage storage) {
        this.repository = repository;
        this.loader = loader;
        this.storage = storage;
        this.reloadData = new ConcurrentHashMap<>();
    }

    public void addForReload(Long id, Double from, int delaySeconds) {
        ReloadData data = new ReloadData();
        data.setFrom(from);
        data.setReloadTime(LocalDateTime.now().plusSeconds(delaySeconds));
        reloadData.put(id, data);
    }

    @Scheduled(fixedRate = 1, timeUnit = TimeUnit.SECONDS)
    private void process() {
        synchronized (reloadData) {
            if (!reloadData.isEmpty()) {
                log.warn("Reload data map: {}", reloadData);
                LocalDateTime now = LocalDateTime.now();
                reloadData.forEach((id, data) -> {
                    if (data.getReloadTime().isBefore(now)) {
                        reload(id, data.getFrom());
                    }
                });
            }
        }
    }

    private void reload(Long id, Double from) {
        log.info("Run reload process for {} from {}", id, from);

        storage.resetById(id);

        repository.deleteItems(id, from);

        reloadData.remove(id);

        loader.applyStatus(id, LoadStatus.WAIT, true);
    }

    @Getter
    @Setter
    private static class ReloadData {
        private Double from;
        private LocalDateTime reloadTime;
    }
}
