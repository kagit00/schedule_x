package com.shedule.x.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EdgeDTO {
    private String fromNodeHash;
    private String toNodeHash;
    private float score;
    private UUID domainId;
    private UUID groupId;
}

