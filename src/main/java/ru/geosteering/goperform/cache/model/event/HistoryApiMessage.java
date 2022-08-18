package ru.geosteering.goperform.cache.model.event;

import lombok.Getter;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;

@Getter
public class HistoryApiMessage extends Event<ApiMessage> {

    private final String subject;

    public HistoryApiMessage(ApiMessage apiMessage, String subject) {
        super(EventType.HISTORY_API_MESSAGE, apiMessage);
        this.subject = subject;
    }
}
