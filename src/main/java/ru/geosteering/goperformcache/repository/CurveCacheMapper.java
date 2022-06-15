package ru.geosteering.goperformcache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperformcache.repository.dto.CurveCacheDTO;
import ru.geosteering.goperformcache.repository.dto.MetaDataDTO;

import java.time.OffsetDateTime;
import java.util.List;

@Mapper
public interface CurveCacheMapper {

    @Insert("insert into curve_cache (curve_id, first, last, cache) values (#{curveId}, #{first}, #{last}, #{cache}::jsonb)")
    void save(CurveCacheDTO curveCacheDTO);

    @Select("select max(last) from curve_cache where curve_id=#{id}")
    OffsetDateTime getMaxLast(@Param("id") Long id);

    @Select("select min(first) from curve_cache where curve_id=#{id}")
    OffsetDateTime getMinFirst(@Param("id") Long id);

    @Select("select curve_id as curveId, min(first) as first, max(last) as last from curve_cache group by curve_id")
    List<MetaDataDTO> getMetaData();

    @Select("select curve_id as curveId, first, last, cache from curve_cache where id = #{id} and ((first between '#{from}' and '#{to}') or (last between '#{from}' and '#{to}'))")
    List<CurveCacheDTO> get(@Param("id") Long id, @Param("from")OffsetDateTime from, @Param("to")OffsetDateTime to);
}
