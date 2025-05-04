package com.shedule.x.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Builder
@AllArgsConstructor
@Data
public class MatchResult {
    private String partnerId;
    private double score;
}
