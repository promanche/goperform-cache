package ru.geosteering.goperform.cache.processor;

import lombok.extern.slf4j.Slf4j;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.nio.charset.StandardCharsets;

@Slf4j
public class CurveStatusNotifier {
    private static final String STATUS_SUBJECT = "curve.status";

    public static void notifyStatus(Long curveId, SingleCurveProcessor.LoadStatus status) {
        CurveDispatcher.CurveStatusMessage statusMsg = new CurveDispatcher.CurveStatusMessage(curveId, status);
        String message = StaticMapper.toJson(statusMsg);
        NatsConnector.publish(STATUS_SUBJECT, message.getBytes(StandardCharsets.UTF_8));
        log.debug("Published status message for curve {}: {}", curveId, status);
    }
} 