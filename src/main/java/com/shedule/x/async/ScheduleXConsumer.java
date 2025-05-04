package com.shedule.x.async;


import com.shedule.x.processors.ScheduleXPayloadProcessor;
import com.shedule.x.validation.GenericValidationUtility;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
public class ScheduleXConsumer {

    private static final String USERS_TOPIC_PATTERN = ".*-users";
    private static final String MATCH_SUGGESTIONS_TRANSFER_JOB_STATUS_TOPIC_PATTERN = ".*-match-suggestions-transfer-job-status-retrieval";
    private final ScheduleXPayloadProcessor scheduleXPayloadProcessor;


    @KafkaListener(
            topicPattern = USERS_TOPIC_PATTERN,
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "4",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeImportedNodes(ConsumerRecord<String, String> consumerRecord) {
        var payload = GenericValidationUtility.validatePayload(consumerRecord);

        try {
            scheduleXPayloadProcessor.processImportedNodesPayload(payload);
        } catch (Exception e) {
            log.error("Failed to process payload: {}", payload, e);
        }
    }

    @KafkaListener(
            topicPattern = MATCH_SUGGESTIONS_TRANSFER_JOB_STATUS_TOPIC_PATTERN,
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "4",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeMatchesExportJobStatus(ConsumerRecord<String, String> consumerRecord) {
        var payload = GenericValidationUtility.validatePayload(consumerRecord);

        try {
            scheduleXPayloadProcessor.processNodesImportJobStatusPayload(payload);
        } catch (Exception e) {
            log.error("Error processing job status payload: {}", payload, e);
        }
    }
}
