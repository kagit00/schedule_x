package com.shedule.x.models;

import com.shedule.x.dto.enums.MatchType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MatchingContext {
    private int sizeOfNodes;
    private boolean isCostBased;
    private boolean isSchedulingBased;
    private boolean isRealTime;
    private String industry;
    private MatchType matchType;
    private UUID domainId;
}
