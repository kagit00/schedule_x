package com.shedule.x.dto;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.enums.NodeType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MatchingRequest {
    private String groupId;
    private LocalDateTime createdAfter;
    private Integer limit;
    private String industry;
    private MatchType matchType;
    private String weightFunctionKey;
    private String partitionKey;
    private String leftPartitionValue;
    private String rightPartitionValue;
    private boolean isRealTime;
    private NodeType nodeType;
    private UUID domainId;
}
