package com.shedule.x.dto;

import com.shedule.x.service.GraphRecords;

import java.util.List;
import java.util.UUID;

public record EdgeWriteTask(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId,
            String cycleId,
            EdgeWriteRequest originalRequest) {}