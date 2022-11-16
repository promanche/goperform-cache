package ru.geosteering.goperform.cache.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.*;
import org.springframework.stereotype.Repository;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.model.ExtraCurveInfo;
import ru.geosteering.goperform.cache.repository.dto.*;
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

    public void saveItems(List<ItemDto> list) {
        log.debug("saveItems started");
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
        log.debug("saveItems completed in {} ms", System.currentTimeMillis() - started);
    }

    public List<String> getItemsFromTo(Long id, Double from, Double to) {
        log.debug("getItemsFromTo started");
        long started = System.currentTimeMillis();
        if (from == null && to == null) {
            return getAllItems(id);
        }
        log.debug("getItemsFromTo completed in {} ms", System.currentTimeMillis() - started);
        return itemsMapper.getFromTo(id, from, to);
    }

    public List<ItemDto> getItemsFromTo(long[] ids, Double from, Double to) {
        log.debug("getItemsFromTo started");
        long started = System.currentTimeMillis();
        StringJoiner joiner = new StringJoiner(",");
        for (Long id : ids) {
            joiner.add(String.valueOf(id));
        }
        log.debug("getItemsFromTo completed in {} ms", System.currentTimeMillis() - started);
        return itemsMapper.getMulti(joiner.toString(), from, to);
    }

    private List<String> getAllItems(Long id) {
        return itemsMapper.getAll(id);
    }

    public void deleteItems(Long id, Double from) {
        log.debug("deleteItems started");
        long started = System.currentTimeMillis();
        if (from == null) {
            itemsMapper.deleteAll(id);
        } else {
            itemsMapper.deleteAfter(id, from);
        }
        log.debug("deleteItems completed in {} ms", System.currentTimeMillis() - started);
    }

    public Optional<CurveItem> getFirstItem(Long id) {
        log.debug("getFirstItem started");
        long started = System.currentTimeMillis();
        Optional<CurveItem> optional = Optional.ofNullable(StaticMapper.parseObject(itemsMapper.getFirst(id), CurveItem.class));
        log.debug("getFirstItem FINISH");
        return optional;
    }

    public Optional<CurveItem> getLastItem(Long id) {
        log.debug("getLastItem started");
        long started = System.currentTimeMillis();
        Optional<CurveItem> optional = Optional.ofNullable(StaticMapper.parseObject(itemsMapper.getLast(id), CurveItem.class));
        log.debug("getLastItem completed in {} ms", System.currentTimeMillis() - started);
        return optional;
    }

    public int getItemsRecordsCount(Long id) {
        log.debug("getItemsRecordsCount started");
        long started = System.currentTimeMillis();
        int count = itemsMapper.getRecordsCount(id);
        log.debug("getItemsRecordsCount completed in {} ms", System.currentTimeMillis() - started);
        return count;
    }

    public void saveOrUpdateInfo(ExtraCurveInfo info) {
        log.debug("saveOrUpdateInfo started");
        long started = System.currentTimeMillis();
        if (curveInfoMapper.exists(info.getId())) {
            curveInfoMapper.update(StaticMapper.toJson(info), info.getId());
        } else {
            curveInfoMapper.save(CurveInfoDto.fromCurveInfo(info));
        }
        log.debug("saveOrUpdateInfo completed in {} ms", System.currentTimeMillis() - started);
    }

    public Optional<ExtraCurveInfo> getInfo(Long id) {
        log.debug("getInfo started");
        long started = System.currentTimeMillis();
        String json = curveInfoMapper.get(id);
        Optional<ExtraCurveInfo> optional = Optional.empty();
        if (json != null) {
            optional = Optional.ofNullable(StaticMapper.parseObject(json, ExtraCurveInfo.class));
        }
        log.debug("getInfo completed in {} ms", System.currentTimeMillis() - started);
        return optional;
    }

    public void saveSegments(List<SegmentDto> list) {
        log.debug("saveSegments started");
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
        log.debug("saveSegments completed in {} ms", System.currentTimeMillis() - started);
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
        log.debug("getScalesLast started");
        long started = System.currentTimeMillis();
        Map<Integer, Double> result = new HashMap<>();

        segmentsMapper.getScalesLast(id)
                .forEach((k, v) -> result.put(k, v.getLast()));

        log.debug("getScalesLast completed in {} ms", System.currentTimeMillis() - started);
        return result;
    }

    public void deleteSegments(Long id, Double from) {
        log.debug("deleteSegments started");
        long started = System.currentTimeMillis();
        if (from == null) {
            segmentsMapper.deleteAll(id);
        } else {
            segmentsMapper.deleteAfter(id, from);
        }
        log.debug("deleteSegments completed in {} ms", System.currentTimeMillis() - started);
    }
}
