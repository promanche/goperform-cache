package ru.geosteering.goperform.cache.nats;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.*;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.processor.CurveDispatcher;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class HistoryMessageHandler implements MessageHandler, ConnectionEventListener {

    private final Config config;
    private final CurveDispatcher curveDispatcher;

    private ExecutorService executor;

    private void initExecutor() {
        executor = Executors.newFixedThreadPool(config.HISTORY_THREADS);
    }

    @Override
    public void onMessage(Message msg) {
        executor.submit(() -> handleMessage(msg));
    }

    private void handleMessage(Message msg) {

        try {

            ApiMessage apiMessage = StaticMapper.parseObject(new String(msg.getData()), ApiMessage.class);

            if (apiMessage != null) {

                ApiMessage.MessageType type = apiMessage.getType();

                switch (type) {
                    case CURVE_DATA:
                        curveDispatcher.onCurveDataMessage((CurveDataMessage) apiMessage, false);
                        break;
                    case DATA_END:
                        curveDispatcher.onDataEndMessage((DataEndMessage) apiMessage, msg.getSubject());
                        break;
                    default:
                        log.info("Some apiMessage: {}", apiMessage);
                        break;
                }
            }

        } catch (Exception e) {
            log.error("Subject: {}, message {}", new String(msg.getData()), msg.getSubject(), e);
        }
    }

    @Override
    public void onConnect() {
        initExecutor();
    }

    @Override
    public void onDisconnect() {
        waitTerminated();
    }

    @PreDestroy
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
