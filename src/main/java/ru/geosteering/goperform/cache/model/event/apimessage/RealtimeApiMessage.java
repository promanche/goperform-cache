package ru.geosteering.goperform.cache.model.event.apimessage;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class RealtimeApiMessage extends Event {

    private final ApiMessage apiMessage;

    public RealtimeApiMessage(ApiMessage apiMessage) {
        super(EventType.REALTIME_API_MESSAGE);
        this.apiMessage = apiMessage;
    }
}
