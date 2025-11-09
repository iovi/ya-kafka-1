package com.example.ya_kafka_1.service;

import com.example.ya_kafka_1.dto.MessageDto;
import com.example.ya_kafka_1.util.RandomMessageUtilService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class RandomMessageProducer {

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    private KafkaProducer<Long, MessageDto> producer;

    private final Random random = new Random();

    @PostConstruct
    public void setUpProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName()); //ключ сериализуется как строка
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName()); //значение сериализуется как json
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); //дожидаемся ответов от всех реплик
        properties.put(ProducerConfig.RETRIES_CONFIG, 100); //сделаем большое количество попыток для отправки at least once
        producer = new KafkaProducer<>(properties);
    }

    @PreDestroy
    public void closeProducer() {
        producer.close();
    }

    @Scheduled(fixedDelay = 1000)
    public void sendRecord() {
        //создание сообщения
        MessageDto messageDto = new MessageDto();
        messageDto.setId(random.nextLong());
        messageDto.setMessageText(Stream.generate(RandomMessageUtilService::getRandomWord)
                .limit(3).collect(Collectors.joining(" "))); //текст из трёх слов

        // отправка сообщения с uuid-ключом
        ProducerRecord<Long, MessageDto> record = new ProducerRecord<>("ya_topic", messageDto.getId(),
                messageDto);
        try {
            producer.send(record, (metadata, e) -> {
                if (e == null) {
                    log.info("Produced and sent: {}", messageDto);
                } else {
                    log.error("Error sending message {}: {}", messageDto, e.getMessage());
                }
            });
        } catch (SerializationException se) {
            log.error("Serialization exception for message {}: {}", messageDto, se.getMessage());
        }

    }
}
