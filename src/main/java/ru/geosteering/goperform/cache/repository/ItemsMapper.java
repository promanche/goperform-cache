package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface ItemsMapper {

    @Insert("insert into items (curve_id, first, last, data) values (#{curveId}, #{first}, #{last}, #{data}::jsonb)")
    void save(ItemDto itemDto);

    @Select("select data from items where curve_id=${id} and ((${from} <= last) and (${to} >= first))")
    List<String> getFromTo(@Param("id") Long id, @Param("from") Double from, @Param("to") Double to);

    @Select("select data from items where curve_id=${id}")
    List<String> getAll(@Param("id") Long id);

    @Delete("delete from items where curve_id=${id} and last >= ${key}")
    void deleteAfter(@Param("id") Long id, @Param("key") Double key);

    @Delete("delete from items where curve_id=${id}")
    void deleteAll(Long id);
}
