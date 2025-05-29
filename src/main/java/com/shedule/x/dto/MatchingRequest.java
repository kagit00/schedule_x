package com.shedule.x.dto;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.validation.ValidEnum;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
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
    @NotBlank
    @NotNull
    private String groupId;
    private LocalDateTime createdAfter;
    private Integer limit;
    private String industry;
    private String processingCycleId;
    private int page;

    @ValidEnum(enumClass = MatchType.class, message = "Invalid match type")
    private MatchType matchType;

    private String weightFunctionKey;
    private String partitionKey;
    private String leftPartitionValue;
    private String rightPartitionValue;
    private boolean isRealTime;

    @ValidEnum(enumClass = NodeType.class, message = "Invalid node type")
    private NodeType nodeType;

    @NotBlank
    @NotNull
    private UUID domainId;

    @Max(value = 100000, message = "number of nodes should not exceed 100k.")
    @Min(value = 5, message = "number of nodes should not be below 5")
    private int numberOfNodes;
}
