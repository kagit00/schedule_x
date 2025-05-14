package com.shedule.x.dto;

import com.shedule.x.processors.PayloadProcessor;
import lombok.Data;

@Data
public class KafkaListenerConfig {
    private String topicPattern;
    private String groupId;
    private int concurrency;
    private String dlqTopic;
    private PayloadProcessor payloadProcessor;
}