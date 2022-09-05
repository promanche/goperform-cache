package ru.geosteering.goperform.cache.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.*;
import org.springframework.stereotype.Repository;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.repository.dto.*;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MainRepository {

    private final ItemsMapper itemsMapper;
    private final MetaDataMapper metaDataMapper;
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

    public List<Long> getAllItemIds() {
        return itemsMapper.getAllIds();
    }

    public void deleteItems(Long id, Double from) {
        if (from == null) {
            itemsMapper.deleteAll(id);
        } else {
            itemsMapper.deleteAfter(id, from);
        }
    }

    public Double getFirstItemKey(Long id) {
        return itemsMapper.getFirst(id);
    }

    public Double getLastItemKey(Long id) {
        return itemsMapper.getLast(id);
    }

    public int getItemsRecords(Long id) {
        return itemsMapper.getRecordsCount(id);
    }

    public void saveOrUpdateMetaData(MetaData metaData) {
        if (metaDataMapper.exists(metaData.getId())) {
            metaDataMapper.update(StaticMapper.toJson(metaData), metaData.getId());
        } else {
            metaDataMapper.save(MetaDataDto.fromMetaData(metaData));
        }
    }

    public List<MetaData> getAllMetaData() {
        return metaDataMapper.getAll().stream()
                .map(str -> StaticMapper.parseObject(str, MetaData.class))
                .collect(Collectors.toList());
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

    public Set<Integer> getSegmentsScales(Long id) {
        return segmentsMapper.getScales(id);
    }
}
