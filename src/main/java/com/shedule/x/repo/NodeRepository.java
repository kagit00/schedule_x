package com.shedule.x.repo;

import com.shedule.x.models.Node;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;


public interface NodeRepository extends JpaRepository<Node, UUID>, NodeRepositoryCustom {
    Optional<Node> findByReferenceIdAndGroupIdAndDomainId(
            @Param("referenceId") String referenceId, @Param("groupId") UUID groupId, @Param("domainId") UUID domainId);

    @QueryHints(@QueryHint(name = "org.hibernate.cacheable", value = "true"))
    @Query("SELECT DISTINCT KEY(m) FROM Node n JOIN n.metaData m WHERE n.groupId = :groupId")
    Set<String> findDistinctMetadataKeysByGroupId(@Param("groupId") UUID groupId);

    @Query("SELECT COUNT(n) FROM Node n WHERE n.domainId = :domainId AND n.groupId = :groupId AND n.processed = true")
    long countByDomainIdAndGroupIdAndProcessedTrue(@Param("domainId") UUID domainId, @Param("groupId") UUID groupId);

    @Query(value = """
    SELECT n.id
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
    List<UUID> findUnprocessedNodeIdsByCursor(
            @Param("groupId") UUID groupId,
            @Param("domainId") UUID domainId,
            @Param("cursorCreatedAt") LocalDateTime cursorCreatedAt,
            @Param("cursorId") UUID cursorId,
            @Param("limit") int limit
    );

    @Query(
            value = "SELECT created_at FROM nodes WHERE id = :nodeId",
            nativeQuery = true
    )
    Optional<LocalDateTime> findCreatedAtById(@Param("nodeId") UUID nodeId);
}