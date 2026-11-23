package dev.fullstack.kafkashare;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "poc.role", havingValue = "publisher")
class Publisher {
    @Bean
    CommandLineRunner publish(KafkaTemplate<String, String> template,
            @Value("${poc.mode}") String mode,
            @Value("${poc.count}") int count,
            @Value("${poc.start}") int first) {
        return args -> {
            if (!mode.equals("share") && !mode.equals("classic")) {
                throw new IllegalArgumentException("POC_MODE must be share or classic");
            }
            String topic = "poc." + mode;
            // One key + one partition make the ordering comparison unambiguous.
            for (int i = first; i < first + count; i++) {
                String id = String.format("%04d", i);
                template.send(topic, "claim-42", id).get(20, TimeUnit.SECONDS);
                System.out.printf("POC EVENT=PRODUCED MODE=%s WORKER=publisher ID=%s "
                    + "PARTITION=0 OFFSET=na AT=%s DURATION_MS=0%n", mode, id, Instant.now());
            }
            System.out.printf("POC EVENT=PUBLISH_COMPLETE MODE=%s COUNT=%d%n", mode, count);
        };
    }
}
