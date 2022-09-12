package ru.geosteering.goperform.cache.nats;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.commonModels.dataService.responses.CurveDataMessage;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.processor.CurveDispatcher;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealtimeMessageHandler implements MessageHandler {

    private final Config config;
    private final CurveDispatcher curveDispatcher;

    private ExecutorService executor;

    @PostConstruct
    public void initExecutor() {
        executor = Executors.newFixedThreadPool(config.REALTIME_THREADS);
    }

    @Override
    public void onMessage(Message msg) {
        executor.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {

        try {
            if (notSpam(msg.getSubject())) {

                ApiMessage apiMessage = StaticMapper.parseObject(new String(msg.getData()), ApiMessage.class);

                if (apiMessage != null) {
                    if (apiMessage.getType() == ApiMessage.MessageType.CURVE_DATA) {

                        curveDispatcher.onCurveDataMessage((CurveDataMessage) apiMessage, true);

                    } else {

                        log.info("Some apiMessage: {}", apiMessage);
                    }
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private boolean notSpam(String subject) {

        String tail = subject.substring(config.SUBJECT.length() + 1);

        try {
            Long.parseLong(tail);
            return true;

        } catch (NumberFormatException e) {
            return false;
        }
    }

    public void waitTerminated() {
        try {
            executor.shutdown();
            while (!executor.isTerminated()) {
                Thread.sleep(100);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
