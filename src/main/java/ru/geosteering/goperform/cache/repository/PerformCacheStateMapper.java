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

    @Update("update perform_cache_state set updated_at=#{updated_at} where curve_id=#{id}")
    void updateTime(@Param("id") Long id, @Param("updated_at") OffsetDateTime updatedAt);

    @Update("update perform_cache_state set well_id=#{well_id} where curve_id=#{id}")
    void updateWellId(@Param("id") Long id, @Param("well_id") String wellId);

    @Select("select * from perform_cache_state")
    @Results(value = {
            @Result(property = "id", column = "curve_id"),
            @Result(property = "updatedAt", column = "updated_at"),
            @Result(property = "wellId", column = "well_id")
    })
    List<PerformCacheState> getAll();
}
