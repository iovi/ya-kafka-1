package com.example.ya_kafka_1.service;

import com.example.ya_kafka_1.dto.MessageDto;
import com.example.ya_kafka_1.util.RandomMessageUtilService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.kafka.support.serializer.JsonSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class RandomProducer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private KafkaProducer<String, MessageDto> producer;

    private final Random random = new Random();

    @PostConstruct
    public void setUpProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    @PreDestroy
    public void closeProducer() {
        producer.close();
    }

    @SneakyThrows
    @Scheduled(fixedRate = 4000)
    public void sendRecord() {
        //создание сообщения
        MessageDto messageDto = new MessageDto();
        messageDto.setId(random.nextLong());
        messageDto.setMessageText(Stream.generate(RandomMessageUtilService::getRandomWord)
                .limit(3).collect(Collectors.joining(" "))); //текст из трёх слов

        //вывод сообщения
        //String jsonString = objectMapper.writeValueAsString(messageDto);
        log.info("produced: {}", messageDto);

        // отправка сообщения с uuid-ключом
        ProducerRecord<String, MessageDto> record = new ProducerRecord<>("ya_topic", UUID.randomUUID().toString(),
                messageDto);
        producer.send(record);
    }
}
