package ru.geosteering.goperform.cache.processor;

import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.CurveItem;

import java.util.List;

public interface DefaultEventProcessor extends EventProcessor {

    @Override
    default void onRealtimeApiMessage(ApiMessage apiMessage) {

    }

    @Override
    default void onHistoryApiMessage(ApiMessage apiMessage, String subject) {

    }

    @Override
    default void onRealtimeCurveItem(Long id, CurveItem item) {

    }

    @Override
    default void onHistoryCurveItem(Long id, CurveItem item) {

    }

    @Override
    default void onItemsBatch(Long id, List<CurveItem> items) {

    }

    @Override
    default void onOldItem(Long id, CurveItem item) {

    }

    @Override
    default void onReloadData(Long id, Double from) {

    }

    @Override
    default void onConnect() {

    }

    @Override
    default void onDisconnect() {

    }

    @Override
    default void onLoadResult(Long id, LoadResult result, Double from, Double to) {

    }
}
