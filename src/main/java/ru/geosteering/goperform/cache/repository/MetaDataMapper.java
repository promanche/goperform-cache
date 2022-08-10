package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.MetaDataDto;

import java.util.List;

@Mapper
public interface MetaDataMapper {

    @Select("select exists (select 1 from meta_data where curve_id=${curveId})")
    boolean exists(@Param("curveId") Long curveId);

    @Insert("insert into meta_data (curve_id, data) values (#{curveId}, #{data}::jsonb)")
    void save(MetaDataDto metaDataDto);

    @Update("update meta_data set data='${data}'::jsonb where curve_id=${curveId}")
    void update(@Param("data") String data, @Param("curveId") Long curveId);

    @Select("select data from meta_data")
    List<String> getAll();
}
