package com.shedule.x.dto;

import lombok.Data;

import java.util.List;

@Data
public class ExportResultMessage {
    private String exportId;
    private String status; // SUCCESS, FAILED
    private String errorMessage;
    private List<MatchResult> matchResults;

    @Data
    public static class MatchResult {
        private String referenceId;
        private String matchedReferenceId;
        private double compatibilityScore;
    }
}