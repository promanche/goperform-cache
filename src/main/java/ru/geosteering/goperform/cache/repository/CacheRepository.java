package ru.geosteering.goperform.cache.repository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.*;
import org.springframework.stereotype.Repository;
import ru.geosteering.goperform.cache.model.CacheItem;

import java.util.List;

@Repository
@AllArgsConstructor
@Slf4j
public class CacheRepository {

    private final CacheMapper mapper;
    private final SqlSessionFactory sessionFactory;

    public void save(List<CacheDTO> list) throws Exception {
        SqlSession session = sessionFactory.openSession(ExecutorType.BATCH);

        try {
            for (CacheDTO dto : list) {
                mapper.save(dto);
            }

            session.commit();
        } catch (Exception e) {
            session.rollback();
            throw new Exception(e);
        } finally {
            session.close();
        }
    }

    public CacheItem getEmptyLast(Long id) {
        return mapper.getEmptyLast(id);
    }

    public List<String> getFromTo(Long id, Double from, Double to) {
        return mapper.getFromTo(id, from, to);
    }

    public List<String> getAll(Long id) {
        return mapper.getAll(id);
    }
}
