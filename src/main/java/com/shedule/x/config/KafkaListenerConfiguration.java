package com.shedule.x.config;

import com.shedule.x.dto.KafkaListenerConfig;
import com.shedule.x.processors.ScheduleXPayloadProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaListenerConfiguration {
        private final ScheduleXPayloadProcessor payloadProcessor;

        public KafkaListenerConfiguration(ScheduleXPayloadProcessor payloadProcessor) {
            this.payloadProcessor = payloadProcessor;
        }

        @Bean
        public KafkaListenerConfig nodesImportConfig() {
            KafkaListenerConfig config = new KafkaListenerConfig();
            config.setTopicPattern(".*-users");
            config.setGroupId("nodes-import-group");
            config.setConcurrency(4);
            config.setDlqTopic("users-import-dlq");
            config.setPayloadProcessor(payloadProcessor::processImportedNodesPayload);
            return config;
        }

        @Bean
        public KafkaListenerConfig jobStatusConfig() {
            KafkaListenerConfig config = new KafkaListenerConfig();
            config.setTopicPattern(".*-match-suggestions-transfer-job-status-retrieval");
            config.setGroupId("matches-job-status-group");
            config.setConcurrency(4);
            config.setDlqTopic("matches-job-status-dlq");
            config.setPayloadProcessor(payloadProcessor::processNodesImportJobStatusPayload);
            return config;
        }
}
