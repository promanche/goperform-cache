package ru.geosteering.goperform.cache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.geosteering.goperform.cache.nats.NatsConnector;

@SpringBootApplication
@EnableScheduling
public class GoperformCacheApplication implements CommandLineRunner {

    @Autowired
    NatsConnector connector;

    public static void main(String[] args) {
        SpringApplication.run(GoperformCacheApplication.class, args);
    }

    @Override
    public void run(String... args) {
        connector.initConnectionScheduler();
    }
}
