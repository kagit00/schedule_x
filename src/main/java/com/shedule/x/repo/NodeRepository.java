package com.shedule.x.repo;

import com.shedule.x.models.Node;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;


public interface NodeRepository extends JpaRepository<Node, UUID>, NodeRepositoryCustom {
    @Query("SELECT n.id FROM Node n WHERE n.groupId = :groupId AND (:createdAfter IS NULL OR n.createdAt >= :createdAfter)")
    List<UUID> findIdsByGroupId(@Param("groupId") String groupId, @Param("createdAfter") LocalDateTime createdAfter, Pageable pageable);

    @Query("SELECT n FROM Node n JOIN FETCH n.metaData WHERE n.id IN :ids")
    List<Node> findByIdsWithMetadata(@Param("ids") List<UUID> ids);

    @Query("SELECT n FROM Node n JOIN FETCH n.metaData WHERE n.groupId = :groupId")
    List<Node> findByGroupId(@Param("groupId") String groupId);

    @Query("SELECT n FROM Node n JOIN FETCH n.metaData WHERE n.groupId = :groupId AND n.createdAt >= :createdAfter")
    List<Node> findFilteredByGroupIdAfter(
            @Param("groupId") String groupId,
            @Param("createdAfter") LocalDateTime createdAfter
    );

    @QueryHints(@QueryHint(name = "org.hibernate.cacheable", value = "true"))
    @Query("SELECT DISTINCT KEY(m) FROM Node n JOIN n.metaData m WHERE n.groupId = :groupId")
    Set<String> findDistinctMetadataKeysByGroupId(@Param("groupId") String groupId);
}