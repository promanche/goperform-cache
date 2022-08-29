package ru.geosteering.goperform.cache.nats;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.geosteering.commonModels.dataService.responses.ApiMessage;
import ru.geosteering.goperform.cache.config.Config;
import ru.geosteering.goperform.cache.processor.EventDispatcher;
import ru.geosteering.goperform.cache.utils.StaticMapper;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@RequiredArgsConstructor
@Slf4j
public class HistoryMessageHandler implements MessageHandler {

    private final Config config;
    private final EventDispatcher dispatcher;

    private ExecutorService executor;

    @PostConstruct
    public void initExecutor() {
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
                dispatcher.onHistoryApiMessage(apiMessage, msg.getSubject());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void waitTerminated(){
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
