package com.example.ya_kafka_1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class YaKafka1Application {

    public static void main(String[] args) {
        SpringApplication.run(YaKafka1Application.class, args);
    }

}
