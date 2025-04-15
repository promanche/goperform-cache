package ru.geosteering.goperform.cache.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.PerformCacheState;
import ru.geosteering.goperform.cache.service.ApiServiceRestClientService;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Класс управления состоянием кривых в кэше.
 * Отслеживает время последнего изменения каждой кривой,
 * определяет устаревшие кривые и обновляет их состояние в базе данных.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CurveStateManager {
    private final MainRepository repository;
    private final ApiServiceRestClientService apiServiceRestClientService;

    /**
     * Мапа для хранения времени последнего изменения каждой кривой.
     * Ключ: ID кривой, Значение: Время последнего изменения.
     */
    private final ConcurrentMap<Long, LocalDateTime> curvesLastChange = new ConcurrentHashMap<>();

    /**
     * Инициализация состояния всех кривых при запуске приложения.
     * Загружает все ID кривых из репозитория и устанавливает их актуальное состояние.
     */
    @EventListener(ApplicationStartedEvent.class)
    public void initCurvesLastChange() {
        List<Long> infoIds = repository.getInfoIds();
        log.info("There are {} curves stored in cache", infoIds.size());

        setCurvesActualState(infoIds);
    }

    /**
     * Периодически обновляет состояние кривых в базе данных.
     * Сохраняет только те кривые, которые были изменены за последние 24 часа.
     */
    @Scheduled(fixedDelay = 3, initialDelay = 1, timeUnit = TimeUnit.HOURS)
    public void updateState() {
        curvesLastChange.forEach((id, lastChange) -> {
            if (lastChange.isAfter(LocalDateTime.now().minusDays(1))) {
                PerformCacheState state = new PerformCacheState(id, lastChange.atOffset(ZoneOffset.UTC), null);
                repository.saveOrUpdateState(state);
            }
        });
        log.debug("State values were updated");
    }

    /**
     * Обновляет время последнего изменения для указанной кривой.
     *
     * @param curveId ID кривой.
     */
    public void changed(Long curveId) {
        curvesLastChange.put(curveId, LocalDateTime.now());
    }

    /**
     * Удаляет состояние кривой.
     *
     * @param curveId ID кривой.
     */
    public void removeState(Long curveId) {
        curvesLastChange.remove(curveId);
    }

    /**
     * Фильтрует записи по времени последнего изменения и возвращает список ID.
     *
     * @param daysToExpire Количество дней до истечения срока.
     * @return Список ID записей, которые считаются устаревшими.
     */
    public List<Long> getExpiredCurves(int daysToExpire) {
        return curvesLastChange.entrySet().stream()
                .filter(entry -> entry.getValue().isBefore(LocalDateTime.now().minusDays(daysToExpire)))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * Устанавливает актуальное состояние для всех кривых.
     * Если состояние уже сохранено в базе данных, оно загружается.
     * Для новых кривых состояние определяется через API.
     *
     * @param infoIds Список ID кривых.
     */
    private void setCurvesActualState(List<Long> infoIds) {
        // Установка состояний для уже сохраненных кривых
        setCurvesStoredState(infoIds);

        // Определение кривых, для которых нет состояния
        Set<Long> savedCurvesWithState = curvesLastChange.keySet();
        List<Long> newCurves = infoIds.stream()
                .filter(id -> !savedCurvesWithState.contains(id))
                .collect(Collectors.toList());

        if (newCurves.isEmpty()) {
            return;
        }

        // Получение состояний новых кривых через API
        Map<ApiServiceRestClientService.WellState, List<Long>> allWellsCurves = apiServiceRestClientService.getAllWellsCurves(newCurves);

        for (Long id : newCurves) {
            LocalDateTime lastChange = determineLastChangeFromApi(allWellsCurves, id);
            if (lastChange != null) {
                curvesLastChange.put(id, lastChange);
                log.debug("Curve {} last change was {}", id, lastChange);

                // Сохранение состояния в репозитории
                Optional<ApiServiceRestClientService.WellState> wellState = allWellsCurves.entrySet().stream()
                        .filter(entry -> entry.getValue().contains(id))
                        .map(Map.Entry::getKey)
                        .findFirst();

                wellState.ifPresent(state ->
                        repository.saveOrUpdateState(
                                new PerformCacheState(id, lastChange.atOffset(ZoneOffset.UTC), state.getWellId().toString())
                        )
                );
            }
        }
    }

    /**
     * Определяет время последнего изменения кривой на основе состояния скважины.
     *
     * @param allWellsCurves Мап состояний скважин и соответствующих им кривых.
     * @param curveId        ID кривой.
     * @return Время последнего изменения кривой или null, если состояние не определено.
     */
    private LocalDateTime determineLastChangeFromApi(Map<ApiServiceRestClientService.WellState, List<Long>> allWellsCurves, Long curveId) {
        for (Map.Entry<ApiServiceRestClientService.WellState, List<Long>> entry : allWellsCurves.entrySet()) {
            if (entry.getValue().contains(curveId)) {
                return switch (entry.getKey().getState()) {
                    case "WELL_GREEN" -> LocalDateTime.now();
                    case "WELL_YELLOW" -> LocalDateTime.now().minusMinutes(10);
                    case "WELL_RED" -> LocalDateTime.now().minusDays(1);
                    case "WELL" -> LocalDateTime.now().minusDays(30);
                    default -> null;
                };
            }
        }
        return null;
    }

    /**
     * Устанавливает состояние для уже сохраненных кривых.
     *
     * @param infoIds Список ID кривых.
     */
    private void setCurvesStoredState(List<Long> infoIds) {
        List<PerformCacheState> performCacheStates = repository.getAllStates();
        performCacheStates.forEach(state -> {
            if (infoIds.contains(state.getId()) && state.getWellId() != null) {
                curvesLastChange.put(state.getId(), state.getUpdatedAt().toLocalDateTime());
            }
        });
    }
}