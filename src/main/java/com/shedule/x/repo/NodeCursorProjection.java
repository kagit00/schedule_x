package com.shedule.x.repo;

import java.time.LocalDateTime;
import java.util.UUID;

public interface NodeCursorProjection {
    UUID getId();
    LocalDateTime getCreatedAt();
}