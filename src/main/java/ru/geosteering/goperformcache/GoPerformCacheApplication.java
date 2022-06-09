package ru.geosteering.goperformcache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.geosteering.goperformcache.nats.NatsConnector;

@SpringBootApplication
@EnableScheduling
public class GoPerformCacheApplication implements CommandLineRunner {

    @Autowired
    NatsConnector connector;

    public static void main(String[] args) {
        SpringApplication.run(GoPerformCacheApplication.class, args);
    }

    @Override
    public void run(String... args) {
        connector.connect();
    }
}
