package com.shedule.x.async;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class ScheduleXProducer {

    private final KafkaTemplate<String, String> statusKafkaTemplate;
    private final KafkaTemplate<String, String> exportKafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final Executor kafkaCallbackExecutor;

    public void sendMessage(String topic, String key, String value, boolean isStatusMessage) {
        KafkaTemplate<String, String> template = isStatusMessage ? statusKafkaTemplate : exportKafkaTemplate;
        long startTime = System.nanoTime();

        template.send(topic, key, value)
                .whenCompleteAsync((result, ex) -> {
                    long durationMs = (System.nanoTime() - startTime) / 1_000_000;

                    if (ex == null) {
                        log.info("Sent message to {}: key={}, duration={} ms", topic, key, durationMs);
                        meterRegistry.timer("kafka_send_duration", "topic", topic, "type", isStatusMessage ? "status" : "export")
                                .record(durationMs, TimeUnit.MILLISECONDS);
                    } else {
                        log.error("Failed to send to {}: key={}, error={}", topic, key, ex.getMessage(), ex);
                        meterRegistry.counter("kafka_send_failures", "topic", topic, "type", isStatusMessage ? "status" : "export")
                                .increment();
                        sendToDlq(key, value, isStatusMessage);
                    }
                }, kafkaCallbackExecutor);
    }

    private void sendToDlq(String key, String value, boolean isStatusMessage) {
        KafkaTemplate<String, String> template = isStatusMessage ? statusKafkaTemplate : exportKafkaTemplate;
        long startTime = System.nanoTime();

        template.send("schedule-x-dlq", key, value)
                .whenCompleteAsync((result, ex) -> {
                    long durationMs = (System.nanoTime() - startTime) / 1_000_000;

                    if (ex == null) {
                        log.info("Sent to DLQ: key={}, duration={} ms", key, durationMs);
                        meterRegistry.timer("kafka_dlq_send_duration", "type", isStatusMessage ? "status" : "export")
                                .record(durationMs, TimeUnit.MILLISECONDS);
                    } else {
                        log.error("Failed to send to DLQ: key={}, error={}", key, ex.getMessage(), ex);
                        meterRegistry.counter("kafka_dlq_send_failures", "type", isStatusMessage ? "status" : "export")
                                .increment();
                    }
                }, kafkaCallbackExecutor);
    }
}
