package com.srini.kafka.springbootkafka.controller;

import com.srini.kafka.springbootkafka.model.SuperHero;
import com.srini.kafka.springbootkafka.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private ProducerService producerService;

    @GetMapping("/publish")// use of request param pulls the message after the ? eg ?message=hello pulls hello into message.
    public String publishMessage(@RequestParam("key") String key, @RequestParam("message") String message){
        producerService.sendMessage(key,message);
        return "Message published successfully with key '" + key + "': " + message;

    }

    @PostMapping("/publish")
    public String publishSuperHero(@RequestBody SuperHero superHero){
        producerService.sendSuperHeroMessage(superHero);
        return "SuperHero Message Published: " + superHero;
    }
}
