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
    }

    public List<String> getItemsFromTo(Long id, Double from, Double to) {
        if (from == null && to == null) {
            return getAllItems(id);
        }
        return itemsMapper.getFromTo(id, from, to);
    }

    private List<String> getAllItems(Long id) {
        return itemsMapper.getAll(id);
    }

    public void deleteItems(Long id, Double from) {
        if (from == null) {
            itemsMapper.deleteAll(id);
        } else {
            itemsMapper.deleteAfter(id, from);
        }
    }

    public Optional<CurveItem> getFirstItem(Long id) {
        String json = itemsMapper.getFirst(id);
        if (json != null) {
            List<CurveItem> items = StaticMapper.parseListOf(itemsMapper.getFirst(id), CurveItem.class);
            if (!items.isEmpty()) {
                return Optional.of(items.get(0));
            }
        }
        return Optional.empty();
    }

    public Optional<CurveItem> getLastItem(Long id) {
        String json = itemsMapper.getLast(id);
        if (json != null) {
            List<CurveItem> items = StaticMapper.parseListOf(itemsMapper.getLast(id), CurveItem.class);
            if (!items.isEmpty()) {
                return Optional.of(items.get(0));
            }
        }
        return Optional.empty();
    }

    public int getItemsRecords(Long id) {
        return itemsMapper.getRecordsCount(id);
    }

    public void saveOrUpdateInfo(ExtraCurveInfo info) {
        if (curveInfoMapper.exists(info.getId())) {
            curveInfoMapper.update(StaticMapper.toJson(info), info.getId());
        } else {
            curveInfoMapper.save(CurveInfoDto.fromCurveInfo(info));
        }
    }

    public Optional<ExtraCurveInfo> getInfo(Long id) {
        String json = curveInfoMapper.get(id);
        if (json != null) {
            return Optional.ofNullable(StaticMapper.parseObject(json, ExtraCurveInfo.class));
        }
        return Optional.empty();
    }

    public void saveSegments(List<SegmentDto> list) {
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
    }

    public List<String> getSegmentsFromTo(Long id, int scale, Double from, Double to) {
        if (from == null && to == null) {
            return getAllSegments(id, scale);
        }
        return segmentsMapper.getFromTo(id, scale, from, to);
    }

    private List<String> getAllSegments(Long id, int scale) {
        return segmentsMapper.getAll(id, scale);
    }

    public Map<Integer, Double> getScalesLast(Long id) {
        Map<Integer, Double> result = new HashMap<>();

        segmentsMapper.getScalesLast(id)
                .forEach((k, v) -> result.put(k, v.getLast()));

        return result;
    }

    public void deleteSegments(Long id, Double from) {
        if (from == null) {
            segmentsMapper.deleteAll(id);
        } else {
            segmentsMapper.deleteAfter(id, from);
        }
    }
}
