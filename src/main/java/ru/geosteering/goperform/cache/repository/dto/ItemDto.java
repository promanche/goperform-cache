package ru.geosteering.goperform.cache.repository.dto;

import lombok.*;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.utils.MapperUtils;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class ItemDto {
    private Long curveId;
    private Double first;
    private Double last;
    private String data;

    public static ItemDto fromItemsList(Long curveId, List<CurveItem> list) {
        ItemDto itemDto = new ItemDto();
        itemDto.setCurveId(curveId);
        itemDto.setFirst(list.get(0).getKey());
        itemDto.setLast(list.get(list.size() - 1).getKey());
        itemDto.setData(MapperUtils.toJson(list));

        return itemDto;
    }
}
