package com.srini.kafka.springbootkafka.config;
// This is the config class it tells how are things wired up.


import com.srini.kafka.springbootkafka.model.SuperHero;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;


import java.util.HashMap;
import java.util.Map;

@Configuration // instructions for creating and configuring the spring-managed  objects(bean).
public class KafkaProducerConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")// telling to read the value from the properties files and store it in this variable.
    private String bootstrapServers;

    // JSON Producer(For superhero objects.
    //<t> doest know the type yet, could change later its a place holder.This method works for any object type.
    public ProducerFactory<String, SuperHero> producerFactory() {
        // configprops is a checklist that you are filling something..put is one item in the checklist.
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);

    }// this methods builds the machine

    @Bean
    public KafkaTemplate<String,SuperHero> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }// this object will be called by the servuice class that send the objects.

    // String Producer for the plain text messages.

    public ProducerFactory<String, String> producerStringFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateStringFactory() {
        return new KafkaTemplate<>(producerStringFactory());
    }

}
