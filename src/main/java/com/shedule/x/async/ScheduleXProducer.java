package com.shedule.x.async;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
public class ScheduleXProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topic, String key, String value) {
        try {
            kafkaTemplate.send(topic, key, value)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send to {}: key={}, error={}", topic, key, ex.getMessage(), ex);
                            sendToDlq(key, value);
                        } else {
                            log.info("Sent message to {}: key={}", topic, key);
                        }
                    });
        } catch (Exception e) {
            log.error("Error sending to {}: key={}, error={}", topic, key, e.getMessage(), e);
            sendToDlq(key, value);
        }
    }

    private void sendToDlq(String key, String value) {
        kafkaTemplate.send("schedule-x-dlq", key, value)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send to DLQ: key={}, error={}", key, ex.getMessage(), ex);
                    } else {
                        log.info("Sent to DLQ: key={}", key);
                    }
                });
    }
}