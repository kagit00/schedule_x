package com.shedule.x.dto;

import com.shedule.x.models.Domain;

import java.util.UUID;

public record GroupTaskRequest(Domain domain, UUID groupId, String cycleId) {}