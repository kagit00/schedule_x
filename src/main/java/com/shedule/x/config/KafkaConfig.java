package com.shedule.x.config;


import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    // Producer Configuration
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        Map<String, Object> producerProps = Map.of(
                "bootstrap.servers", bootstrapServers,
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName(),
                "batch.size", 16384,
                "linger.ms", 1,
                "retries", 3,
                "retry.backoff.ms", 100
        );
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    // Consumer Configuration
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> consumerProps = Map.of(
                "bootstrap.servers", bootstrapServers,
                "group.id", groupId,
                "auto.offset.reset", autoOffsetReset,
                "key.deserializer", StringDeserializer.class.getName(),
                "value.deserializer", StringDeserializer.class.getName()
        );
        return new DefaultKafkaConsumerFactory<>(consumerProps);
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(2);

        var backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000L);

        var errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate(), (record, exception) -> new TopicPartition("schedule-x-dlq", -1)), backOff);
        errorHandler.addNotRetryableExceptions(InvalidTopicException.class);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
