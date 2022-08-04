package ru.geosteering.goperform.cache.repository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.*;
import org.springframework.stereotype.Repository;
import ru.geosteering.goperform.cache.model.MetaData;
import ru.geosteering.goperform.cache.utils.MapperUtils;

import java.util.List;
import java.util.stream.Collectors;

@Repository
@AllArgsConstructor
@Slf4j
public class MainRepository {

    private final ItemsMapper itemsMapper;
    private final MetaDataMapper metaDataMapper;
    private final SegmentsMapper segmentsMapper;
    private final SqlSessionFactory sessionFactory;

    public void saveItems(List<ItemDto> list) throws Exception {
        SqlSession session = sessionFactory.openSession(ExecutorType.BATCH);

        try {
            for (ItemDto dto : list) {
                itemsMapper.save(dto);
            }

            session.commit();
        } catch (Exception e) {
            session.rollback();
            throw new Exception(e);
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

    public void saveOrUpdateMetaData(MetaData metaData) {
        if (metaDataMapper.exists(metaData.getId())) {
            metaDataMapper.update(MapperUtils.toJson(metaData), metaData.getId());
        } else {
            metaDataMapper.save(MetaDataDto.fromMetaData(metaData));
        }
    }

    public List<MetaData> getAllMetaData() {
        return metaDataMapper.getAll().stream()
                .map(MapperUtils::parseMetaData)
                .collect(Collectors.toList());
    }

    public void saveLines(SegmentDto segmentDto) {
        segmentsMapper.save(segmentDto);
    }

    public List<String> getLinesFromTo(Long id, int scale, Double from, Double to) {
        if (from == null && to == null) {
            return getAllLines(id, scale);
        }
        return segmentsMapper.getFromTo(id, scale, from, to);
    }

    private List<String> getAllLines(Long id, int scale) {
        return segmentsMapper.getAll(id, scale);
    }
}
