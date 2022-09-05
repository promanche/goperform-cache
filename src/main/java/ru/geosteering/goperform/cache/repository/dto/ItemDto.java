package ru.geosteering.goperform.cache.repository.dto;

import lombok.*;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class ItemDto {
    private Long id;
    private Double first;
    private Double last;
    private String data;

    public static ItemDto fromItemsList(Long id, List<CurveItem> list) {
        ItemDto itemDto = new ItemDto();
        itemDto.setId(id);
        itemDto.setFirst(list.get(0).getKey());
        itemDto.setLast(list.get(list.size() - 1).getKey());
        itemDto.setData(StaticMapper.toJson(list));

        return itemDto;
    }
}
