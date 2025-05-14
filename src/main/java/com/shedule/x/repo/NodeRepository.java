package com.shedule.x.repo;

import com.shedule.x.models.Node;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Repository
public interface NodeRepository extends JpaRepository<Node, UUID> {

    @Query("SELECT n FROM Node n WHERE n.groupId = :groupId")
    List<Node> findByGroupId(@Param("groupId") String groupId, Pageable pageable);

    @Query("SELECT n FROM Node n WHERE n.groupId = :groupId AND n.createdAt >= :createdAfter")
    List<Node> findFilteredByGroupIdAfter(
            @Param("groupId") String groupId,
            @Param("createdAfter") LocalDateTime createdAfter,
            Pageable pageable
    );

    @QueryHints(@QueryHint(name = "org.hibernate.cacheable", value = "true"))
    @Query("SELECT DISTINCT KEY(m) FROM Node n JOIN n.metaData m WHERE n.groupId = :groupId")
    Set<String> findDistinctMetadataKeysByGroupId(@Param("groupId") String groupId);
}
