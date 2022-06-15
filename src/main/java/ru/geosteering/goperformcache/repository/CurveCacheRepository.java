package ru.geosteering.goperformcache.repository;

import lombok.AllArgsConstructor;
import org.apache.ibatis.session.*;
import org.springframework.stereotype.Repository;
import ru.geosteering.goperformcache.repository.dto.CurveCacheDTO;
import ru.geosteering.goperformcache.repository.dto.MetaDataDTO;

import java.time.OffsetDateTime;
import java.util.List;

@Repository
@AllArgsConstructor
public class CurveCacheRepository {

    private final CurveCacheMapper mapper;
    private final SqlSessionFactory sessionFactory;

    public void save(List<CurveCacheDTO> list) throws Exception {
        SqlSession session = sessionFactory.openSession(ExecutorType.BATCH);
        try {
            for (CurveCacheDTO dto : list) {
                session.getMapper(CurveCacheMapper.class)
                        .save(dto);
            }

            session.commit();
        } catch (Exception e) {
            session.rollback();
            throw new Exception(e);
        } finally {
            session.close();
        }
    }

    public OffsetDateTime getMaxLast(Long id) {
        return mapper.getMaxLast(id);
    }

    public OffsetDateTime getMinFirst(Long id) {
        return mapper.getMinFirst(id);
    }

    public List<MetaDataDTO> getMetaData() {
        return mapper.getMetaData();
    }

    public List<CurveCacheDTO> get(Long id, OffsetDateTime from, OffsetDateTime to) {
        return mapper.get(id, from, to);
    }
}
