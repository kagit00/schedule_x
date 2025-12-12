package com.shedule.x.repo;

import com.shedule.x.models.Node;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import org.springframework.stereotype.Repository;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Repository
public interface NodeRepository extends JpaRepository<Node, UUID> {

    // 1. Core Lookups
    Optional<Node> findByReferenceIdAndGroupIdAndDomainId(
            @Param("referenceId") String referenceId,
            @Param("groupId") UUID groupId,
            @Param("domainId") UUID domainId);

    // 2. Metadata Keys (Cached)
    @QueryHints(@QueryHint(name = "org.hibernate.cacheable", value = "true"))
    @Query("SELECT DISTINCT KEY(m) FROM Node n JOIN n.metaData m WHERE n.groupId = :groupId")
    Set<String> findDistinctMetadataKeysByGroupId(@Param("groupId") UUID groupId);

    @Query(value = """
        SELECT CAST(n.id AS uuid) as id, n.created_at as createdAt
        FROM nodes n
        WHERE n.group_id = :groupId
          AND n.domain_id = :domainId
          AND n.is_processed = false
          AND (
                CAST(:cursorCreatedAt AS timestamp) IS NULL
                OR (n.created_at, n.id) > (CAST(:cursorCreatedAt AS timestamp), :cursorId)
              )
        ORDER BY n.created_at ASC, n.id ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<NodeCursorProjection> findUnprocessedNodeIdsAndDatesByCursor(
            @Param("groupId") UUID groupId,
            @Param("domainId") UUID domainId,
            @Param("cursorCreatedAt") LocalDateTime cursorCreatedAt,
            @Param("cursorId") UUID cursorId,
            @Param("limit") int limit
    );

    // 4. Eager Fetching (Hydration)
    @Query("SELECT DISTINCT n FROM Node n LEFT JOIN FETCH n.metaData WHERE n.id IN :ids")
    List<Node> findByIdsWithMetadata(@Param("ids") Collection<UUID> ids);

    // 5. Batch Updates
    @Modifying(clearAutomatically = true)
    @Query("UPDATE Node n SET n.processed = true WHERE n.id IN :ids")
    void markAsProcessed(@Param("ids") Collection<UUID> ids);

    @Modifying(clearAutomatically = true)
    @Query("UPDATE Node n SET n.processed = true WHERE n.referenceId IN :referenceIds AND n.domainId = :domainId")
    void markAsProcessedByReferenceId(@Param("referenceIds") Collection<String> referenceIds, @Param("domainId") UUID domainId);

    @Query("SELECT COUNT(n) FROM Node n WHERE n.domainId = :domainId AND n.groupId = :groupId AND n.processed = true")
    long countByDomainIdAndGroupIdAndProcessedTrue(@Param("domainId") UUID domainId, @Param("groupId") UUID groupId);

    @Query("SELECT COUNT(n) FROM Node n WHERE n.domainId = :domainId AND n.groupId = :groupId")
    long countByDomainIdAndGroupId(@Param("domainId") UUID domainId, @Param("groupId") UUID groupId);
}