package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.CurveInfoDto;

import java.util.List;

@Mapper
public interface CurveInfoMapper {

    @Select("select exists (select 1 from info where curve_id=#{id})")
    boolean exists(@Param("id") Long id);

    @Insert("insert into info (curve_id, data) values (#{id}, #{data}::jsonb)")
    void save(CurveInfoDto curveInfoDto);

    @Update("update info set data=#{data}::jsonb where curve_id=#{id}")
    void update(@Param("data") String data, @Param("id") Long id);

    @Select("select data from info where curve_id=#{id}")
    String get(@Param("id") Long id);

    @Select("select data from info where curve_id in (${ids})")
    List<String> getAllByIds(@Param("ids") String ids);

    @Select("select curve_id from info")
    List<Long> getAllIds();

    @Delete("delete from info where curve_id=#{id}")
    void delete(@Param("id") Long id);
}
