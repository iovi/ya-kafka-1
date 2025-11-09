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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


@Slf4j
@Service
public class BatchMessageConsumer {
    private KafkaConsumer<String, MessageDto> consumer2;

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    @PostConstruct
    public void setUpConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //ключ десериализуется как строка
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName()); //значение десериализуется как json
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.ya_kafka_1.dto"); //пакет с dto значения должен быть доверенным для десериализации
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "3000"); //максимальное количество байт за один poll
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //автоматически offset не применяем
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");
        consumer2 = new KafkaConsumer<>(properties);

        // Подписка на топик
        consumer2.subscribe(Collections.singletonList("ya_topic"));
    }

    @PreDestroy
    public void closeProducer() {
        consumer2.close();
    }

    @Scheduled(fixedDelay = 10000)
    public void getBatch() {
        try {
            ConsumerRecords<String, MessageDto> records = consumer2.poll(Duration.ofMillis(100));
            int i = 0, count = records.count();

            if (count >= 10) {
                for (ConsumerRecord<String, MessageDto> record : records) {
                    log.info("BatchMessageConsumer consumed: {} - {}", i++, record.value());
                }
                consumer2.commitSync();
            }
        } catch (RecordDeserializationException rde) {
            log.error("BatchMessageConsumer deserialization error: {}", rde.getMessage());
        }
    }
}
