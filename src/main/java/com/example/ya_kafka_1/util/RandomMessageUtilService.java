package com.example.ya_kafka_1.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RandomMessageUtilService {

    private static final List<String> DICTIONARY = Arrays.asList("apple", "banana", "cherry", "date", "elderberry");

    public static String getRandomWord() {
        Random random = new Random();
        int randomIndex = random.nextInt(DICTIONARY.size());
        return DICTIONARY.get(randomIndex);
    }
}
