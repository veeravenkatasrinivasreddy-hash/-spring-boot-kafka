package com.srini.kafka.springbootkafka.service;


import com.srini.kafka.springbootkafka.model.SuperHero;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;



@Service
public class ProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);

    @Value("${spring.kafka.topic}")
    private String topic;

    @Value("${spring.kafka.superhero-topic}")
    private String superheroTopic;

    @Autowired// depedency injection you declare what you need adn spring figures it out how to supply it.
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, SuperHero> kafkaTemplateSuperHero;

    // actual sending logic
    public void sendMessage(String key,String message){
        LOGGER.info("####  -> Publishing Message with key '{}' -> {}",key, message);
        kafkaTemplate.send(topic,key, message);// actual moment where messages leave the app and gets pushed into the broker into the topic name.
    }

    public void sendSuperHeroMessage(SuperHero superHero){
        LOGGER.info("####  -> Publishing SuperHero Message -> {}", superHero);
        kafkaTemplateSuperHero.send(superheroTopic, superHero);
    }
}
