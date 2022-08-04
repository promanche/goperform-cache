package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface SegmentsMapper {

    @Insert("insert into segments (curve_id, scale, first, last, data) values (#{curveId}, #{scale}, #{first}, #{last}, #{data}::jsonb)")
    void save(SegmentDto segmentDto);

    @Select("select data from segments where curve_id=${id} and scale=${scale} and ((${from} <= last) and (${to} >= first))")
    List<String> getFromTo(@Param("id") Long id, @Param("scale") int scale, @Param("from") Double from, @Param("to") Double to);

    @Select("select data from segments where curve_id=${id} and scale=${scale}")
    List<String> getAll(@Param("id") Long id, @Param("scale") int scale);
}
