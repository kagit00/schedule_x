package com.shedule.x.dto;

import com.shedule.x.dto.enums.NodeType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder
@AllArgsConstructor
@Data
public class MatchTransfer {
    private String referenceId;
    private String matchedReferenceId;
    private Double compatibilityScore;
    private String groupId;
    private NodeType type;
    private String matchSuggestionType;
}
