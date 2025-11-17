package com.shedule.x.dto;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public record CursorPage(
        List<UUID> ids,
        boolean hasMore,
        LocalDateTime lastCreatedAt,
        UUID lastId) {}