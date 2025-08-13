package ru.geosteering.goperform.cache.processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.nats.NatsConnector;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class CurveStatusNotifier {
    private static String STATUS_SUBJECT_PREFIX = "curves.status.csv";

    @Autowired
    public void setConfig(Config config) {
        STATUS_SUBJECT_PREFIX = config.GOPERFORM_STATUS_TOPIC;
    }

    public static void notifyStatus(Long curveId, SingleCurveProcessor.LoadStatus status) {
        CurveDispatcher.CurveStatusMessage statusMsg = new CurveDispatcher.CurveStatusMessage(curveId, status);
        String message = StaticMapper.toJson(statusMsg);
        String subject = STATUS_SUBJECT_PREFIX + curveId;
        NatsConnector.publish(subject, message.getBytes(StandardCharsets.UTF_8));
        log.debug("Published status message for curve {}: {}", curveId, status);
    }
} 