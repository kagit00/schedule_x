package com.shedule.x.dto;

import lombok.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class NodeDTO {
    private UUID id;
    private String type;
    private String referenceId;
    @Builder.Default
    private Map<String, String> metaData = new HashMap<>();
    private UUID groupId;
    private LocalDateTime createdAt;
    private UUID domainId;
    private boolean processed;
}
