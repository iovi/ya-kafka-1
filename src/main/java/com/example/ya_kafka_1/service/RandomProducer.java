package com.example.ya_kafka_1.service;

import com.example.ya_kafka_1.dto.Message;
import com.example.ya_kafka_1.util.RandomMessageUtilService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class RandomProducer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Scheduled(fixedRate = 5000)
    public void checkRecords() {
        Message message = new Message();
        message.setUid(UUID.randomUUID().toString());
        message.setMessageText(Stream.generate(RandomMessageUtilService::getRandomWord)
                .limit(3).collect(Collectors.joining(" ")));
        String jsonString = objectMapper.writeValueAsString(message);
        log.info(jsonString);
    }
}
