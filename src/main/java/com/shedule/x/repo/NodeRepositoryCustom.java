package com.shedule.x.repo;

import com.shedule.x.models.Node;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface NodeRepositoryCustom {
    List<UUID> findIdsByGroupIdAndDomainId(String groupId, UUID domainId, Pageable pageable, LocalDateTime createdAfter);
    CompletableFuture<List<Node>> findByIdsWithMetadataAsync(List<UUID> ids);
    CompletableFuture<List<Node>> findByGroupIdAsync(String groupId);
    CompletableFuture<List<Node>> findFilteredByGroupIdAfterAsync(String groupId, LocalDateTime createdAfter);
    CompletableFuture<Set<String>> findDistinctMetadataKeysByGroupIdAsync(String groupId);
    void markAsProcessed(List<UUID> ids);
}