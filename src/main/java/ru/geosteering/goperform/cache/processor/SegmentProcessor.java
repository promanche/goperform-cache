package ru.geosteering.goperform.cache.processor;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.CurveSegment;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.repository.MainRepository;
import ru.geosteering.goperform.cache.repository.dto.SegmentDto;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class SegmentProcessor {
    protected final ExtraCurveInfo info;
    protected final Config config;
    private final SingleCurveProcessor curveProcessor;
    private final MainRepository repository;
    @Getter
    private final HashMap<Integer, List<CurveSegment>> segmentCache = new HashMap<>();

    private int totalItems;
    private int invalidItems;

    private boolean isApproximatedScale(Integer scale) {
        return scale != null && scale >= config.SEGMENT_SCALE_MIN;
    }

    /**
     * Восстановить состояние подсчёта "штрихов" (агрегатов точек для отображения малых масштабов).
     * Поднимает из БД кэшированные точки, штрихи для которых не были сохранены.
     */
    public void restoreScaledSegments() {
        Map<Integer, Double> lastByScales = repository.getScalesLast(info.getId());

        for (Integer scale : config.SCALE_MINUTES) {
            if (isApproximatedScale(scale)) {
                lastByScales.putIfAbsent(scale, Double.MIN_VALUE);
            }
        }

        Double from = lastByScales.values()
                .stream()
                .min(Double::compareTo)
                .orElse(Double.MIN_VALUE);

        List<CurveItem> items = repository.getItemsFromTo(info.getId(), from, Double.MAX_VALUE)
                .stream()
                .flatMap(str -> StaticMapper.parseListOf(str, CurveItem.class).stream())
                .toList();

        log.info("{} cached items for {} re-scaling loaded", items.size(), info.getId());

        lastByScales.forEach((scale, last) -> {
            List<CurveItem> lostItems = items.stream()
                    .filter(i -> Double.compare(i.getKey(), last) >= 0)
                    .collect(Collectors.toList());

            if (!lostItems.isEmpty()) {
                addItems(lostItems, scale);
            }
        });
        logResults("restoreScaledSegments()");
    }

    public List<CurveSegment> getSegmentFromCache(Double from, Double to, Integer scale) {
        if (!segmentCache.containsKey(scale))
            return Collections.emptyList();

        var segments = segmentCache.get(scale);

        if (from == null && to == null) {
            return segments;
        }

        return segments.stream()
                .filter(segment -> Double.compare(segment.getFirstKey(), from) >= 0
                        && Double.compare(segment.getLastKey(), to) < 0)
                .toList();

    }

    public void logResults(String label) {
        if (invalidItems > 0) {
            log.info("Curve {} segmentation for {}, items invalid/total: {}/{}", info.getId(), label, invalidItems, totalItems);
        }
    }

    public void addItems(Collection<CurveItem> items) {
        config.SCALE_MINUTES.forEach(scale -> addItems(items, scale));
    }

    public void addItems(Collection<CurveItem> items, int scale) {
        if (!isApproximatedScale(scale) || curveProcessor.getFirstSaved() == null || curveProcessor.getLastSaved() == null)
            return;

        List<CurveSegment> segments = segmentCache.computeIfAbsent(scale, k -> new ArrayList<>());
        addItemsToSegments(items, scale, segments);
        if (segments.size() > config.BATCH_SIZE) {
            CurveSegment last = segments.remove(segments.size() - 1);
            saveSegments(segments, scale);
            log.trace("{} segments saved: curve id {}, scale {}, seconds/pxl {}", segments.size(), info.getId(), scale, scale * 60 / 120);
            segments.clear();
            segments.add(last);
        }
    }

    /**
     * Проверяет, принадлежит ли элемент указанному сегменту.
     *
     * @param item    Элемент для проверки.
     * @param segment Сегмент для проверки.
     * @return {@code true}, если элемент принадлежит сегменту; иначе {@code false}.
     */
    private boolean itemBelongsToSegment(CurveItem item, CurveSegment segment) {
        return Double.compare(item.getKey(), segment.getFirstKey()) >= 0
                && Double.compare(item.getKey(), segment.getLastKey()) <= 0;
    }

    /**
     * Добавляет элементы в существующие сегменты или создаёт новые сегменты для них.
     *
     * @param items    Коллекция элементов для добавления.
     * @param scale    Масштаб для создания сегментов.
     * @param segments Список сегментов, куда добавляются элементы.
     */
    public void addItemsToSegments(Collection<CurveItem> items, int scale, List<CurveSegment> segments) {
        CurveSegment lastSegment = segments.isEmpty() ? null : segments.get(segments.size() - 1);
        for (CurveItem item : items) {
            try {
                totalItems++;
                if (lastSegment != null && itemBelongsToSegment(item, lastSegment)) {
                    lastSegment.addItem(item);

                } else {
                    CurveSegment segment = new CurveSegment(item, scale);
                    if (lastSegment != null && Double.compare(lastSegment.getLastKey(), segment.getFirstKey()) == 0) {
                        lastSegment.addItem(item);
                    }
                    segment.addItem(item);
                    segments.add(segment);
                    lastSegment = segment;
                }
            } catch (Exception e) {
                if (invalidItems == 0) {
                    log.info("Create segment exception. Message: {}, item: {}", e.getMessage(), StaticMapper.toJson(item));
                }
                invalidItems++;
                log.trace(e.getMessage(), e);
            }
        }
    }

    private void saveSegments(List<CurveSegment> segments, int scale) {
        List<SegmentDto> transfer = new ArrayList<>();
        int first = 0;
        while (segments.size() - first > config.BATCH_SIZE) {
            transfer.add(SegmentDto.fromLinesList(info.getId(), scale, segments.subList(first, first + config.BATCH_SIZE)));
            first = first + config.BATCH_SIZE;
        }

        transfer.add(SegmentDto.fromLinesList(info.getId(), scale, segments.subList(first, segments.size())));
        repository.saveSegments(transfer);
    }

}