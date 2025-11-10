package com.example.ya_kafka_1.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class MessageDto {

    private Long id;

    private String messageText;

}
