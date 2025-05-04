package com.shedule.x.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NodesTransferJobExchange {
    private UUID jobId;
    private String groupId;
    private UUID domainId;
    private List<String> successList;
    private List<String> failedList;
    private String status;
    private int processed;
    private int total;
}