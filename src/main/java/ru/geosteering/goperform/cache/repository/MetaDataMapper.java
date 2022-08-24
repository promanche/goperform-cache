package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.MetaDataDto;

import java.util.List;

@Mapper
public interface MetaDataMapper {

    @Select("select exists (select 1 from meta_data where curve_id=${id})")
    boolean exists(@Param("id") Long id);

    @Insert("insert into meta_data (curve_id, data) values (#{id}, #{data}::jsonb)")
    void save(MetaDataDto metaDataDto);

    @Update("update meta_data set data='${data}'::jsonb where curve_id=${id}")
    void update(@Param("data") String data, @Param("id") Long id);

    @Select("select data from meta_data")
    List<String> getAll();
}
