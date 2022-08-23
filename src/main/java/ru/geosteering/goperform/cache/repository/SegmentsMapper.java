package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.SegmentDto;

import java.util.List;
import java.util.Set;

@Mapper
public interface SegmentsMapper {

    @Insert("insert into segments (curve_id, scale, first, last, data) values (#{curveId}, #{scale}, #{first}, #{last}, #{data}::jsonb)")
    void save(SegmentDto segmentDto);

    @Select("select data from segments where curve_id=${id} and scale=${scale} and ((${from} <= last) and (${to} >= first)) order by first")
    List<String> getFromTo(@Param("id") Long id, @Param("scale") int scale, @Param("from") Double from, @Param("to") Double to);

    @Select("select data from segments where curve_id=${id} and scale=${scale} order by first")
    List<String> getAll(@Param("id") Long id, @Param("scale") int scale);

    @Select("select max(last) from segments where curve_id=${id} and scale=${scale}")
    Double getLast(@Param("id") Long id, @Param("scale") int scale);

    @Delete("delete from segments where curve_id=${id} and last >= ${key}")
    void deleteAfter(@Param("id") Long id, @Param("key") Double key);

    @Delete("delete from segments where curve_id=${id}")
    void deleteAll(@Param("id") Long id);

    @Select("select distinct scale from segments where curve_id=${id}")
    Set<Integer> getScales(@Param("id") Long id);
}
