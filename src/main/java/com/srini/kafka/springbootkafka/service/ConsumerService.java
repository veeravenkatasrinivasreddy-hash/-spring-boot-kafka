package com.srini.kafka.springbootkafka.service;


import com.srini.kafka.springbootkafka.model.SuperHero;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;



@Service
public class ConsumerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    // String consumer

    @KafkaListener(topics = {"${spring.kafka.topic}"},containerFactory = "kafkaListenerStringFactory", groupId = "group_id")
    public void consumeMessage(List<String> messages, Acknowledgment ack) throws InterruptedException {
        LOGGER.info("####  -> Consuming Message in Batch -> {}", messages.size());
        for (String message : messages){
            if(message.equals("poison")){
                throw new RuntimeException("Simulated processing failure for message:" + message);
            }
            LOGGER.info("####  -> Consuming Message -> {}", message);
        }
        LOGGER.info("####  ->  Simulating slow processing .. sleeping for 10 seconds");
        //Thread.sleep(10000); // gives ua a window to kill the app before ack.
        ack.acknowledge();
        LOGGER.info("####  -> offset committed for this batch");
    }

    //Object consumer
    @KafkaListener(topics = {"${spring.kafka.superhero-topic}"},containerFactory = "kafkaListenerJsonFactory",groupId = "group_id")
    public void consumeSuperHeroMessage(List<SuperHero> superHeroes, Acknowledgment ack) {
        LOGGER.info("####  -> Consuming SuperHero batch Messages -> {}", superHeroes.size());
        for(SuperHero superHero : superHeroes){
            LOGGER.info("####  -> Consuming SuperHero message -> {}", superHero);
        }
        ack.acknowledge();
        LOGGER.info("####  -> offset committed for this batch");
    }
}
