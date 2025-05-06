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
public class MatchSuggestionsExchange {
    private UUID domainId;
    private String groupId;
    private String filePath;
    private String fileName;
    private String contentType;
    private List<String> referenceIds;
}
