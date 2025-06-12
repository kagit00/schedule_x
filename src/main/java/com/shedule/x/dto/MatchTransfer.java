package com.shedule.x.dto;

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
    private String matchSuggestionType;
}
