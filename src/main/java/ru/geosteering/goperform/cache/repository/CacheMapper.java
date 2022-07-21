package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface CacheMapper {

    @Insert("insert into curve_cache (curve_id, type, first, last, cache) values (#{curveId}, #{type}, #{first}, #{last}, #{cache}::jsonb)")
    void saveCacheDTO(CacheDTO cacheDTO);

    @Select("select cache from curve_cache where curve_id=${id} and ((${from} <= last) and (${to} >= first))")
    List<String> getFromTo(@Param("id") Long id, @Param("from") Double from, @Param("to") Double to);

    @Select("select cache from curve_cache where curve_id=${id}")
    List<String> getAllCaches(@Param("id") Long id);

    @Select("select curve_id as curveId, type, max(last) as last from curve_cache group by curve_id, type")
    List<CacheDTO> getAllLast();

    @Select("select curve_id as curveId, type, max(last) as last from curve_cache where curve_id=${id} group by curve_id, type")
    CacheDTO getLast(@Param("id") Long id);

    @Delete("delete from curve_cache where curve_id=${id} and last >= ${key}")
    void deleteAfter(@Param("id") Long id, @Param("key") Double key);

    @Delete("delete from curve_cache where curve_id=${id}")
    void deleteAll(Long id);
}
