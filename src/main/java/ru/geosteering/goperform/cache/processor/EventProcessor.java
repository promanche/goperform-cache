package ru.geosteering.goperform.cache.processor;

import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.CurveItem;
import ru.geosteering.goperform.cache.processor.LoadResult;

import java.util.List;

public interface EventProcessor {

    void onRealtimeApiMessage(ApiMessage apiMessage);

    void onHistoryApiMessage(ApiMessage apiMessage, String subject);

    void onRealtimeCurveItem(Long id, CurveItem item);

    void onHistoryCurveItem(Long id, CurveItem item);

    void onItemsBatch(Long id, List<CurveItem> items);

    void onOldItem(Long id, CurveItem item);

    void onReloadData(Long id);

    void onConnect();

    void onDisconnect();

    void onLoadResult(Long id, LoadResult result);
}
