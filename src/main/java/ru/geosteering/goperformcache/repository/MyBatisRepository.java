package ru.geosteering.goperformcache.repository;

import org.apache.ibatis.annotations.*;

import java.time.OffsetDateTime;
import java.util.List;

@Mapper
public interface MyBatisRepository {

    @Insert("insert into curve_cache (curve_id, first, last, cache) values (#{id}, #{first}, #{last}, #{data}::jsonb)")
    void saveDataBatch(@Param("id") Long id,
                       @Param("first") OffsetDateTime first,
                       @Param("last") OffsetDateTime last,
                       @Param("data") String data);

    @Select("select max(last) from curve_cache where curve_id=#{id}")
    OffsetDateTime getLastTimeByCurveId(@Param("id") Long id);

    @Select("select min(first) from curve_cache where curve_id=#{id}")
    OffsetDateTime getFirstTimeByCurveId(@Param("id") Long id);

    @Select("select curve_id as curveId, min(first) as first, max(last) as last from curve_cache group by curve_id")
    List<MetaDataDTO> getMetaData();

}
