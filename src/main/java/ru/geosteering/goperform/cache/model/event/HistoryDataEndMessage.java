package ru.geosteering.goperform.cache.model.event;

import lombok.Getter;

@Getter
public class HistoryDataEndMessage extends Event<HistoryDataEndMessage.LoadResult> {

    private final Long curveId;

    public HistoryDataEndMessage(Long curveId, LoadResult result) {
        super(EventType.HISTORY_END_MESSAGE, result);
        this.curveId = curveId;
    }

    public enum LoadResult {
        DONE, PART, ERROR
    }
}
