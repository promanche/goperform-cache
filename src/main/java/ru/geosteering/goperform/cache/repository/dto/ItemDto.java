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
    private Double minValue;
    private Double maxValue;
    private String data;
    private Long requestedTimestamp;

    public static ItemDto fromItemsList(Long id, List<CurveItem> list, boolean isNumeric) {
        ItemDto itemDto = new ItemDto();
        itemDto.setId(id);
        itemDto.setFirst(list.get(0).getKey());
        itemDto.setLast(list.get(list.size() - 1).getKey());

        if (isNumeric) {
            Double[] minMaxValue = findMinMaxValue(list);
            itemDto.setMinValue(minMaxValue[0]);
            itemDto.setMaxValue(minMaxValue[1]);
        }

        itemDto.setData(StaticMapper.toJson(list));

        return itemDto;
    }

    private static Double getDoubleValue(CurveItem item) {
        try {
            return ((Number) item.getValue()).doubleValue();
        } catch (Exception e) {
            return null;
        }
    }

    private static Double[] findMinMaxValue(List<CurveItem> items) {
        Double min = null;
        Double max = null;
        for (CurveItem item : items) {
            Double current = getDoubleValue(item);

            if (min == null) {
                min = current;
            } else if (current != null && Double.compare(min, current) > 0) {
                min = current;
            }

            if (max == null) {
                max = current;
            } else if (current != null && Double.compare(max, current) < 0) {
                max = current;
            }
        }
        return new Double[]{min, max};
    }
}
