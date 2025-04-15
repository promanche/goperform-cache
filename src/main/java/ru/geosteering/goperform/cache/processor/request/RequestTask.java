package ru.geosteering.goperform.cache.processor.request;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@AllArgsConstructor
public class RequestTask {

    Long curveId;
    @Setter
    RequestType type;
    RequestJob requestJob;

    int getPriority() {
        return type.priority;
    }

}