package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.PerformCacheState;

import java.time.OffsetDateTime;
import java.util.List;

@Mapper
public interface PerformCacheStateMapper {

    @Select("select exists (select 1 from perform_cache_state where curve_id=#{id})")
    boolean exists(@Param("id") Long id);

    @Insert("insert into perform_cache_state (curve_id, updated_at, well_id) values (#{id}, #{updatedAt}, #{wellId})")
    void save(PerformCacheState performCacheState);

    @Update("update perform_cache_state set updated_at=#{updatedAt} where curve_id=#{id}")
    void update(@Param("id") Long id, @Param("updated_at") OffsetDateTime updatedAt);

    @Select("select * from perform_cache_state")
    List<PerformCacheState> getAll();

    @Delete("delete from perform_cache_state where curve_id=#{id}")
    void delete(@Param("id") Long id);

}
