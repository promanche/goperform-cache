package ru.geosteering.goperform.cache;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.lang.management.ManagementFactory;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class GoperformCacheApplication {

    public static void main(String[] args) {
        for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            log.info("Jvm arg {}", arg);
        }

        SpringApplication.run(GoperformCacheApplication.class, args);
    }
}
