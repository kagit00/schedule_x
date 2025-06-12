package com.shedule.x.config.factory;

import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.UUID;

public interface KeysetPageable extends Pageable {
        UUID getLastId();
        LocalDateTime getLastCreatedAt();
    }