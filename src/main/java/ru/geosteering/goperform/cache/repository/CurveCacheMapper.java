package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.model.CacheItem;

import java.util.List;

@Mapper
public interface CurveCacheMapper {

    @Insert("insert into curve_cache (curve_id, type, first, last, cache) values (#{curveId}, #{type}, #{first}, #{last}, #{cache}::jsonb)")
    void save(CurveCacheDTO curveCacheDTO);

    @Select("select type, max(last) as key from curve_cache where curve_id=#{id} group by type")
    CacheItem getEmptyLast(@Param("id") Long id);

    @Select("select curve_id as curveId, type, first, last, cache from curve_cache where curve_id=${id} and ((${from} <= last) and (${to} >= first))")
    List<CurveCacheDTO> get(@Param("id") Long id, @Param("from") Double from, @Param("to") Double to);
}
