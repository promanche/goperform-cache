package ru.geosteering.goperform.cache.model.event;

import ru.geosteering.commonModels.dataService.responses.ApiMessage;

public class RealTimeApiMessage extends Event<ApiMessage> {

    public RealTimeApiMessage(ApiMessage apiMessage) {
        super(EventType.REALTIME_API_MESSAGE, apiMessage);
    }
}
