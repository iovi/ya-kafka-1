package com.example.ya_kafka_1.service;

import com.example.ya_kafka_1.dto.MessageDto;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Service
public class SingleMessageConsumer {

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    private KafkaConsumer<Long, MessageDto> consumer;

    private final String workingPeriodMs = "1000";

    @PostConstruct
    public void setUpConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName()); //ключ десериализуется как строка
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName()); //значение десериализуется как json
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.ya_kafka_1.dto"); //пакет с dto значения должен быть доверенным для десериализации
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");  // выбираем по одной записи
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // применение offset автоматическое
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, workingPeriodMs); // автоматическое применение каждые ... мс
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        consumer = new KafkaConsumer<>(properties);

        // Подписка на топик
        consumer.subscribe(Collections.singletonList("ya_topic"));
    }

    @PreDestroy
    public void closeProducer() {
        consumer.close();
    }

    @Scheduled(fixedDelayString = workingPeriodMs)
    public void getSingleMessage() {
        try {
            ConsumerRecords<Long, MessageDto> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                int count = records.count();
                if (count > 1) {
                    log.error("SingleMessageConsumer got too many records: {}", count);
                }
                for (ConsumerRecord<Long, MessageDto> record : records) {
                    log.info("SingleMessageConsumer consumed: {}", record.value());
                }
            }
        } catch (RecordDeserializationException rde) {
            log.error("SingleMessageConsumer deserialization error: {}", rde.getMessage());
        }
    }
}
