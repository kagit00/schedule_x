package com.shedule.x.repo;

import com.shedule.x.models.Node;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface NodeRepositoryCustom {
    List<UUID> findIdsByGroupIdAndDomainId(UUID groupId, UUID domainId, Pageable pageable, LocalDateTime createdAfter);
    CompletableFuture<List<Node>> findByIdsWithMetadataAsync(List<UUID> ids);
    CompletableFuture<List<Node>> findByGroupIdAsync(UUID groupId);
    CompletableFuture<List<Node>> findFilteredByGroupIdAfterAsync(UUID groupId, LocalDateTime createdAfter);
    CompletableFuture<Set<String>> findDistinctMetadataKeysByGroupIdAsync(UUID groupId);
    void markAsProcessed(List<UUID> ids);
    void markAsProcessedByReferenceId(List<String> referenceIds, UUID domainId);
}