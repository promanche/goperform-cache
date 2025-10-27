package ru.geosteering.goperform.cache.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.stereotype.Repository;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.repository.dto.CurveInfoDto;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;
import ru.geosteering.goperform.cache.repository.dto.PerformCacheState;
import ru.geosteering.goperform.cache.repository.dto.SegmentDto;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.*;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MainRepository {

    private final ItemsMapper itemsMapper;
    private final CurveInfoMapper curveInfoMapper;
    private final SegmentsMapper segmentsMapper;
    private final SqlSessionFactory sessionFactory;
    private final PerformCacheStateMapper stateMapper;

    //Items

    public void saveItems(List<ItemDto> list) {
        log.trace("saveItems started");
        long started = System.currentTimeMillis();
        SqlSession session = sessionFactory.openSession(ExecutorType.BATCH);

        try {
            for (ItemDto dto : list) {
                itemsMapper.save(dto);
            }

            session.commit();
        } catch (Exception e) {
            session.rollback();
            log.error("Database exception: {}", e.getMessage(), e);
        } finally {
            session.close();
        }
        log.trace("saveItems: saved {} element(s) in {} ms", list.size(), System.currentTimeMillis() - started);
    }

    public List<String> getItemsFromTo(Long curveId, Double from, Double to) {
        log.debug("getItemsFromTo started");
        long started = System.currentTimeMillis();
        List<String> result;
        if (from == null && to == null) {
            result = getAllItems(curveId);
        } else {
            result = itemsMapper.getFromTo(curveId, from, to);
        }
        log.debug("getItemsFromTo completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    public List<ItemDto> getItemsWithLimit(Long curveId, Double first, Integer limit) {
        log.debug("getItemsWithLimit started");
        long started = System.currentTimeMillis();
        List<ItemDto> result;

        result = itemsMapper.getBatchByFirst(curveId, first, limit);

        log.debug("getItemsFromTo completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    public List<ItemDto> getItemsFromTo(long[] ids, Double from, Double to) {
        log.debug("getItemsFromTo[] started");
        long started = System.currentTimeMillis();
        StringJoiner joiner = new StringJoiner(",");
        for (Long id : ids) {
            joiner.add(String.valueOf(id));
        }
        List<ItemDto> result = itemsMapper.getMulti(joiner.toString(), from, to);
        log.debug("getItemsFromTo[] completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    private List<String> getAllItems(Long id) {
        return itemsMapper.getAll(id);
    }

    public void deleteItems(Long id, Double from) {
        log.trace("deleteItems started");
        long started = System.currentTimeMillis();
        if (from == null) {
            itemsMapper.deleteAll(id);
        } else {
            itemsMapper.deleteAfter(id, from);
        }
        log.trace("deleteItems completed in {} ms", System.currentTimeMillis() - started);
    }

    public boolean deleteBatchItems(Long id) {
        boolean result = false;
        log.trace("deleteBatchItems started");
        long started = System.currentTimeMillis();
        int count = itemsMapper.getRecordsCount(id);
        if (count > 0) {
            itemsMapper.deleteBatch(id);
            result = true;
        }
        log.trace("deleteBatchItems completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    public Optional<CurveItem> getFirstItem(Long id) {
        log.trace("getFirstItem started");
        long started = System.currentTimeMillis();
        Optional<CurveItem> optional = Optional.ofNullable(StaticMapper.parseObject(itemsMapper.getFirst(id), CurveItem.class));
        log.trace("getFirstItem completed in {} ms", System.currentTimeMillis() - started);
        return optional;
    }

    public Optional<CurveItem> getLastItem(Long id) {
        log.trace("getLastItem started");
        long started = System.currentTimeMillis();
        Optional<CurveItem> optional = Optional.ofNullable(StaticMapper.parseObject(itemsMapper.getLast(id), CurveItem.class));
        log.trace("getLastItem completed in {} ms", System.currentTimeMillis() - started);
        return optional;
    }

    public int getItemsRecordsCount(Long id) {
        log.trace("getItemsRecordsCount started");
        long started = System.currentTimeMillis();
        int count = itemsMapper.getRecordsCount(id);
        log.trace("getItemsRecordsCount completed in {} ms", System.currentTimeMillis() - started);
        return count;
    }

    public Double getItemsMinValue(Long id) {
        return itemsMapper.getMinValue(id);
    }

    public Double getItemsMaxValue(Long id) {
        return itemsMapper.getMaxValue(id);
    }

    public Optional<CurveItem> getItemAtOrBeforeIndex(Long id, Double index) {
        log.trace("getItemAtOrBeforeIndex started for index {}", index);
        long started = System.currentTimeMillis();
        
        // First try to find an exact match in the range
        String exactMatch = itemsMapper.getItemAtIndex(id, index);
        if (exactMatch != null) {
            List<CurveItem> items = StaticMapper.parseListOf(exactMatch, CurveItem.class);
            // Find the exact match or closest before in the items array
            Optional<CurveItem> result = findExactOrClosestBefore(items, index);
            log.trace("getItemAtOrBeforeIndex completed in {} ms", System.currentTimeMillis() - started);
            return result;
        }
        
        // If no exact match, find the item before the index
        String beforeMatch = itemsMapper.getItemBeforeIndex(id, index);
        if (beforeMatch != null) {
            List<CurveItem> items = StaticMapper.parseListOf(beforeMatch, CurveItem.class);
            // Get the last item from this batch (closest to the index)
            if (!items.isEmpty()) {
                Optional<CurveItem> result = Optional.of(items.get(items.size() - 1));
                log.trace("getItemAtOrBeforeIndex completed in {} ms", System.currentTimeMillis() - started);
                return result;
            }
        }
        
        log.trace("getItemAtOrBeforeIndex completed in {} ms (no match)", System.currentTimeMillis() - started);
        return Optional.empty();
    }
    
    private Optional<CurveItem> findExactOrClosestBefore(List<CurveItem> items, Double targetIndex) {
        CurveItem closestBefore = null;
        for (CurveItem item : items) {
            if (item.getKey() != null) {
                if (item.getKey().equals(targetIndex)) {
                    // Exact match found
                    return Optional.of(item);
                } else if (item.getKey() < targetIndex) {
                    // This item is before the target, keep track of it
                    if (closestBefore == null || item.getKey() > closestBefore.getKey()) {
                        closestBefore = item;
                    }
                }
            }
        }
        return Optional.ofNullable(closestBefore);
    }

    //Info

    public void saveOrUpdateInfo(ExtraCurveInfo info) {
        log.trace("saveOrUpdateInfo started");
        long started = System.currentTimeMillis();
        if (curveInfoMapper.exists(info.getId())) {
            curveInfoMapper.update(StaticMapper.toJson(info), info.getId());
        } else {
            curveInfoMapper.save(CurveInfoDto.fromCurveInfo(info));
        }
        log.trace("saveOrUpdateInfo completed in {} ms", System.currentTimeMillis() - started);
    }

    public Optional<ExtraCurveInfo> getInfo(Long id) {
        log.trace("getInfo started");
        long started = System.currentTimeMillis();
        String json = curveInfoMapper.get(id);
        Optional<ExtraCurveInfo> optional = Optional.empty();
        if (json != null) {
            optional = Optional.ofNullable(StaticMapper.parseObject(json, ExtraCurveInfo.class));
        }
        log.trace("getInfo completed in {} ms", System.currentTimeMillis() - started);
        return optional;
    }

    public List<ExtraCurveInfo> getInfos(List<Long> ids) {
        log.trace("getInfos started");
        long started = System.currentTimeMillis();
        StringJoiner joiner = new StringJoiner(",");
        for (Long id : ids) {
            joiner.add(String.valueOf(id));
        }
        List<ExtraCurveInfo> infos = curveInfoMapper.getAllByIds(joiner.toString()).stream()
                .map(e -> StaticMapper.parseObject(e, ExtraCurveInfo.class))
                .toList();
        log.trace("getInfos completed in {} ms", System.currentTimeMillis() - started);
        return infos;
    }

    public List<Long> getInfoIds() {
        log.trace("getInfoIds started");
        long started = System.currentTimeMillis();
        List<Long> all = curveInfoMapper.getAllIds();
        log.trace("getInfoIds completed in {} ms", System.currentTimeMillis() - started);
        return all;
    }

    public void deleteInfo(Long id) {
        curveInfoMapper.delete(id);
    }

    //Segments

    public void saveSegments(List<SegmentDto> list) {
        log.trace("saveSegments started");
        long started = System.currentTimeMillis();
        SqlSession session = sessionFactory.openSession(ExecutorType.BATCH);

        try {
            for (SegmentDto dto : list) {
                segmentsMapper.save(dto);
            }

            session.commit();
        } catch (Exception e) {
            session.rollback();
            log.error("Database exception: {}", e.getMessage(), e);
        } finally {
            session.close();
        }
        log.trace("saveSegments: {} element(s) saved in {} ms", list.size(), System.currentTimeMillis() - started);
    }

    public List<String> getSegmentsFromTo(Long id, int scale, Double from, Double to) {
        log.debug("getSegmentsFromTo started");
        long started = System.currentTimeMillis();
        List<String> result;
        if (from == null && to == null) {
            result = getAllSegments(id, scale);
        } else {
            result = segmentsMapper.getFromTo(id, scale, from, to);
        }
        log.debug("getSegmentsFromTo completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    public List<SegmentDto> getSegmentsFromTo(long[] ids, int scale, Double from, Double to) {
        log.debug("getSegmentsFromTo started");
        long started = System.currentTimeMillis();
        StringJoiner joiner = new StringJoiner(",");
        for (Long id : ids) {
            joiner.add(String.valueOf(id));
        }
        List<SegmentDto> result = segmentsMapper.getMulti(joiner.toString(), scale, from, to);
        log.debug("getSegmentsFromTo completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    private List<String> getAllSegments(Long id, int scale) {
        return segmentsMapper.getAll(id, scale);
    }

    public Map<Integer, Double> getScalesLast(Long id) {
        log.trace("getScalesLast started");
        long started = System.currentTimeMillis();
        Map<Integer, Double> result = new HashMap<>();

        segmentsMapper.getScalesLast(id)
                .forEach((k, v) -> result.put(k, v.getLast()));

        log.trace("getScalesLast completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    public void deleteSegments(Long id, Double from) {
        log.trace("deleteSegments started");
        long started = System.currentTimeMillis();
        if (from == null) {
            segmentsMapper.deleteAll(id);
        } else {
            segmentsMapper.deleteAfter(id, from);
        }
        log.trace("deleteSegments completed in {} ms", System.currentTimeMillis() - started);
    }

    public boolean deleteBatchSegments(Long id) {
        log.trace("deleteBatchSegments started");
        boolean result = false;
        long started = System.currentTimeMillis();
        int count = segmentsMapper.getRecordsCount(id);
        if (count > 0) {
            segmentsMapper.deleteBatch(id);
            result = true;
        }
        log.trace("deleteBatchSegments completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    public int getSegmentsRecordsCount(Long id) {
        return segmentsMapper.getRecordsCount(id);
    }

    // State

    public void saveOrUpdateState(PerformCacheState state) {
        log.trace("saveOrUpdateState started");
        long started = System.currentTimeMillis();
        if (stateMapper.exists(state.getId())) {
            if (state.getWellId() == null) {
                stateMapper.updateTime(state.getId(), state.getUpdatedAt());
            } else {
                stateMapper.updateWellId(state.getId(), state.getWellId());
            }
        } else {
            stateMapper.save(state);
        }
        log.trace("saveOrUpdateState completed in {} ms", System.currentTimeMillis() - started);
    }

    public List<PerformCacheState> getAllStates() {
        return stateMapper.getAll();
    }
}
