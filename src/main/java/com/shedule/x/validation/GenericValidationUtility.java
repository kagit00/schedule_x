package com.shedule.x.validation;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.exceptions.BadRequestException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
@UtilityClass
public final class GenericValidationUtility {

    public static String validatePayload(ConsumerRecord<String, String> consumerRecord, ScheduleXProducer dlqProducer) {
        String payload = consumerRecord.value();
        return payload;
    }
}
