package com.shedule.x.async;


import com.shedule.x.async.consumers.BaseKafkaConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class ScheduleXConsumer extends BaseKafkaConsumer {
    public ScheduleXConsumer(ScheduleXProducer dlqProducer, MeterRegistry meterRegistry, @Qualifier("generalTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        super(dlqProducer, meterRegistry, taskExecutor);
    }

    @KafkaListener(
            topicPattern = "#{@nodesImportConfig.topicPattern}",
            groupId = "#{@nodesImportConfig.groupId}",
            concurrency = "#{@nodesImportConfig.concurrency}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeNodesImport(ConsumerRecord<String, String> consumerRecord) {
        consume(consumerRecord, getListenerConfigs().get(0));
    }

    @KafkaListener(
            topicPattern = "#{@jobStatusConfig.topicPattern}",
            groupId = "#{@jobStatusConfig.groupId}",
            concurrency = "#{@jobStatusConfig.concurrency}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeJobStatus(ConsumerRecord<String, String> consumerRecord) {
        consume(consumerRecord, getListenerConfigs().get(1));
    }
}