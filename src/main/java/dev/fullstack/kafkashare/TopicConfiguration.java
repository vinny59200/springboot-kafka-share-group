package dev.fullstack.kafkashare;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
class TopicConfiguration {
    // ONE partition is essential: with classic groups only one worker can own it.
    @Bean
    NewTopic shareTopic() {
        return TopicBuilder.name("poc.share").partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic classicTopic() {
        return TopicBuilder.name("poc.classic").partitions(1).replicas(1).build();
    }
}
