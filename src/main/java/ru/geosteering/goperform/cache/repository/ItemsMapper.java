package ru.geosteering.goperform.cache.repository;

import org.apache.ibatis.annotations.*;
import ru.geosteering.goperform.cache.repository.dto.ItemDto;

import java.util.List;

@Mapper
public interface ItemsMapper {

    @Insert("insert into items (curve_id, first, last, min_value, max_value, data) values (#{id}, #{first}, #{last}, #{minValue}, #{maxValue}, #{data}::jsonb)")
    void save(ItemDto itemDto);

    @Select("select data from items where curve_id=#{id} and #{from} <= last and #{to} >= first order by first")
    List<String> getFromTo(@Param("id") Long id, @Param("from") Double from, @Param("to") Double to);

    @Select("select curve_id as id, first, last, min_value as minValue, max_value as maxValue, data from items where curve_id in (${ids}) and #{from} <= last and #{to} >= first order by first")
    List<ItemDto> getMulti(@Param("ids") String ids, @Param("from") Double from, @Param("to") Double to);

    @Select("select data from items where curve_id=#{id} order by first")
    List<String> getAll(@Param("id") Long id);

    @Select({
            "<script>",
            "SELECT data, first ",
            "FROM items ",
            "WHERE curve_id = #{curveId} ",
            "  AND first > #{lastFirst} ",
            "ORDER BY first ",
            "LIMIT #{limit}",
            "</script>"
    })
    List<ItemDto> getBatchByFirst(@Param("curveId") Long curveId, @Param("lastFirst") Double lastFirst, @Param("limit") Integer limit);

    @Delete("delete from items where curve_id=#{id} and last >= #{key}")
    void deleteAfter(@Param("id") Long id, @Param("key") Double key);

    @Delete("delete from items where curve_id=#{id}")
    void deleteAll(@Param("id") Long id);

    @Select("select data -> 0 from items where curve_id=#{id} order by first limit 1")
    String getFirst(@Param("id") Long id);

    @Select("select data -> -1 from items where curve_id=#{id} order by last desc limit 1")
    String getLast(@Param("id") Long id);

    @Select("select count(*) from items where curve_id=#{id}")
    int getRecordsCount(@Param("id") Long id);

    @Select("select min(min_value) from items where curve_id=#{id}")
    Double getMinValue(@Param("id") Long id);

    @Select("select max(max_value) from items where curve_id=#{id}")
    Double getMaxValue(@Param("id") Long id);

    @Delete("with batch as (select id from items where curve_id=#{id} limit 1000 for update skip locked) delete from items using batch where items.id=batch.id")
    void deleteBatch(@Param("id") Long id);

    @Select({
            "<script>",
            "SELECT data ",
            "FROM items ",
            "WHERE curve_id = #{curveId} ",
            "  AND first &lt;= #{timestamp} ",
            "ORDER BY first DESC ",
            "LIMIT 1",
            "</script>"
    })
    String getItemBatchByTimestamp(@Param("curveId") Long curveId, @Param("timestamp") Long timestamp);
}
