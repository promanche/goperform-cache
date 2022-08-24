package ru.geosteering.goperform.cache.model.event.apimessage;

import lombok.Getter;
import lombok.ToString;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.model.event.Event;

@Getter
@ToString(callSuper = true)
public class HistoryApiMessage extends Event {

    private final String subject;
    private final ApiMessage apiMessage;

    public HistoryApiMessage(ApiMessage apiMessage, String subject) {
        super(EventType.HISTORY_API_MESSAGE);
        this.subject = subject;
        this.apiMessage = apiMessage;
    }
}
