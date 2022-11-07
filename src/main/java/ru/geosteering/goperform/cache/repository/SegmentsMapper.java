package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.SegmentDto;

import java.util.*;

@Mapper
public interface SegmentsMapper {

    @Insert("insert into segments (curve_id, scale, first, last, data) values (#{id}, #{scale}, #{first}, #{last}, #{data}::jsonb)")
    void save(SegmentDto segmentDto);

    @Select("select data from segments where curve_id=${id} and scale=${scale} and ${from} <= last and ${to} >= first order by first")
    List<String> getFromTo(@Param("id") Long id, @Param("scale") int scale, @Param("from") Double from, @Param("to") Double to);

    @Select("select curve_id as id, scale, first, last, data from segments where curve_id in (${ids}) and scale=${scale} and ${from} <= last and ${to} >= first order by first")
    List<SegmentDto> getMulti(@Param("ids") String ids, @Param("scale") int scale, @Param("from") Double from, @Param("to") Double to);

    @Select("select data from segments where curve_id=${id} and scale=${scale} order by first")
    List<String> getAll(@Param("id") Long id, @Param("scale") int scale);

    @Delete("delete from segments where curve_id=${id} and last >= ${key}")
    void deleteAfter(@Param("id") Long id, @Param("key") Double key);

    @Delete("delete from segments where curve_id=${id}")
    void deleteAll(@Param("id") Long id);

    @Select("select scale, max(last) as last from segments where curve_id=${id} group by scale")
    @MapKey("scale")
    Map<Integer, SegmentDto> getScalesLast(@Param("id") Long id);
}
