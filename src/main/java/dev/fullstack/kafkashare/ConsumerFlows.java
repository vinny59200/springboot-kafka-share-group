package dev.fullstack.kafkashare;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ShareKafkaMessageListenerContainer;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "poc.role", havingValue = "consumer")
class ConsumerFlows {
    @Value("${spring.kafka.bootstrap-servers}")
    private String brokers;

    @Value("${poc.worker-id}")
    private String workerId;

    @Value("${poc.auto-renew-locks}")
    private boolean autoRenewLocks;

    private Map<String, Object> consumerProps(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, workerId);
        return props;
    }

    @Bean
    @ConditionalOnProperty(name = "poc.mode", havingValue = "share")
    IntegrationFlow shareFlow(ProcessingService processor) {
        Map<String, Object> props = consumerProps("poc-share-group");
        // Kafka 4.2: enforce one acquired record per poll, not the default soft batch limit.
        props.put("share.acquire.mode", "record_limit");
        ContainerProperties containerProps = new ContainerProperties("poc.share");
        containerProps.setShareAckMode(ContainerProperties.ShareAckMode.EXPLICIT);
        // For processing > 30s, use Spring Kafka 4.2's background lock renewal.
        containerProps.setShareAutoRenewLocks(autoRenewLocks);
        ShareKafkaMessageListenerContainer<String, String> container =
            new ShareKafkaMessageListenerContainer<>(
                new DefaultShareConsumerFactory<>(props), containerProps);
        container.setConcurrency(1);
        container.setClientId(workerId);
        return IntegrationFlow
            .from(Kafka.shareMessageDrivenChannelAdapter(container).id("shareAdapter"))
            .handle(processor::process)
            .get();
    }

    @Bean
    @ConditionalOnProperty(name = "poc.mode", havingValue = "classic")
    IntegrationFlow classicFlow(ProcessingService processor) {
        Map<String, Object> props = consumerProps("poc-classic-group");
        // auto.offset.reset is supported by classic consumer groups, not share groups.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return IntegrationFlow
            .from(Kafka.messageDrivenChannelAdapter(
                    new DefaultKafkaConsumerFactory<String, String>(props),
                    KafkaMessageDrivenChannelAdapter.ListenerMode.record,
                    "poc.classic")
                .configureListenerContainer(c -> c
                    .id("classicContainer")
                    .concurrency(1)))
            .handle(processor::process)
            .get();
    }
}
