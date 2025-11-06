package com.example.ya_kafka_1.service;

import com.example.ya_kafka_1.dto.MessageDto;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Service
public class SingleMessageConsumer {

    private KafkaConsumer<String, MessageDto> consumer;

    @PostConstruct
    public void setUpConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.ya_kafka_1.dto");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "200");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        consumer = new KafkaConsumer<>(properties);

        // Подписка на топик
        consumer.subscribe(Collections.singletonList("ya_topic"));
    }

    @PreDestroy
    public void closeProducer() {
        consumer.close();
    }

    @Scheduled(fixedRate = 5000)
    public void getSingleMessage() {
        ConsumerRecords<String, MessageDto> records = consumer.poll(Duration.ofMillis(900));
        if (!records.isEmpty()) {
            int count = records.count();
            if (count > 1) {
                log.error("SingleMessageConsumer got too many records: {}", count);
            }
            for (ConsumerRecord<String, MessageDto> record : records) {
                log.info("SingleMessageConsumer consumed: {}", record.value());
            }
        }
    }
}
