package com.shedule.x.async.consumers;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.dto.KafkaListenerConfig;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.concurrent.TimeUnit;


@Slf4j
@RequiredArgsConstructor
public abstract class BaseKafkaConsumer {

    private final ScheduleXProducer dlqProducer;
    private final MeterRegistry meterRegistry;
    private final ThreadPoolTaskExecutor taskExecutor;

    private List<KafkaListenerConfig> listenerConfigs;

    @Autowired
    public void setListenerConfigs(List<KafkaListenerConfig> listenerConfigs) {
        this.listenerConfigs = listenerConfigs;
    }

    public void consume(ConsumerRecord<String, String> consumerRecord, KafkaListenerConfig config) {
        String payload = consumerRecord.value();
        String topic = consumerRecord.topic();
        String key = consumerRecord.key();
        String groupId = config.getGroupId();

        if (payload == null || payload.isBlank()) {
            log.warn("Empty or null payload for key={} on topic={}. Sending to DLQ.", key, topic);
            sendToDlq(config, consumerRecord);
            meterRegistry.counter("kafka_dlq_messages", "topic", topic, "groupId", groupId).increment();
            return;
        }

        long startTime = System.currentTimeMillis();

        CompletableFuture.runAsync(() -> {
            try {
                config.getPayloadProcessor().process(payload).join();
                long duration = System.currentTimeMillis() - startTime;
                meterRegistry.timer("kafka_processing_time", "topic", topic, "groupId", groupId)
                        .record(duration, TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                log.error("Processing failed for key={} on topic={}", key, topic, ex);
                sendToDlq(config, consumerRecord);
                meterRegistry.counter("kafka_processing_errors", "topic", topic, "groupId", groupId).increment();
            }
        }, taskExecutor).orTimeout(120, TimeUnit.SECONDS).exceptionally(ex -> {
            log.error("Async failure for key={} on topic={}", key, topic, ex);
            meterRegistry.counter("kafka_async_errors", "topic", topic, "groupId", groupId).increment();
            return null;
        });
    }

    private void sendToDlq(KafkaListenerConfig config, ConsumerRecord<String, String> record) {
        dlqProducer.sendMessage(config.getDlqTopic(), record.key(), record.value(), false);
    }

    protected List<KafkaListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }
}
