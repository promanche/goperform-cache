package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;

import java.util.List;

@Mapper
public interface ItemsMapper {

    @Insert("insert into items (curve_id, first, last, data) values (#{id}, #{first}, #{last}, #{data}::jsonb)")
    void save(ItemDto itemDto);

    @Select("select data from items where curve_id=${id} and ((${from} <= last) and (${to} >= first)) order by first")
    List<String> getFromTo(@Param("id") Long id, @Param("from") Double from, @Param("to") Double to);

    @Select("select data from items where curve_id=${id} order by first")
    List<String> getAll(@Param("id") Long id);

    @Delete("delete from items where curve_id=${id} and last >= ${key}")
    void deleteAfter(@Param("id") Long id, @Param("key") Double key);

    @Delete("delete from items where curve_id=${id}")
    void deleteAll(@Param("id") Long id);

    @Select("select distinct curve_id from items")
    List<Long> getAllIds();

    @Select("select min(first) from items where curve_id=${id}")
    Double getFirst(@Param("id") Long id);

    @Select("select max(last) from items where curve_id=${id}")
    Double getLast(@Param("id") Long id);

    @Select("select count(*) from items where curve_id=${id}")
    int getRecordsCount(@Param("id") Long id);
}
