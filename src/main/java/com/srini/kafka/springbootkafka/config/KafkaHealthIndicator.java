package com.srini.kafka.springbootkafka.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaHealthIndicator implements HealthIndicator {

    @Override
    public Health health() {
        try(AdminClient adminClient = AdminClient.create(
                Collections.singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))){
            adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
            return Health.up().withDetail("kafka","Connected").build();
        } catch (InterruptedException | ExecutionException | TimeoutException e){
            return Health.down().withDetail("kafka","Not Connected").withException(e).build();
        }

    }
}
