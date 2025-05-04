package com.shedule.x.utils.basic;

import lombok.extern.slf4j.Slf4j;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
public final class DefaultValuesPopulator {


    private DefaultValuesPopulator() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    public static LocalDateTime getCurrentTimestamp() {
        return LocalDateTime.now();
    }


    public static String getUid() {
        return UUID.randomUUID().toString();
    }
}
