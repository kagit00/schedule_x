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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;


public interface NodeRepository extends JpaRepository<Node, UUID>, NodeRepositoryCustom {
    Optional<Node> findByReferenceIdAndGroupIdAndDomainId(
            @Param("referenceId") String referenceId, @Param("groupId") UUID groupId, @Param("domainId") UUID domainId);

    @Query("SELECT DISTINCT n.domainId FROM Node n WHERE n.groupId = :groupId")
    UUID findDomainIdByGroupID(@Param("groupId") String groupId);

    @QueryHints(@QueryHint(name = "org.hibernate.cacheable", value = "true"))
    @Query("SELECT DISTINCT KEY(m) FROM Node n JOIN n.metaData m WHERE n.groupId = :groupId")
    Set<String> findDistinctMetadataKeysByGroupId(@Param("groupId") UUID groupId);

    @Query("SELECT COUNT(n) FROM Node n WHERE n.domainId = :domainId AND n.groupId = :groupId")
    long countByDomainIdAndGroupId(@Param("domainId") UUID domainId, @Param("groupId") String groupId);

    @Query("SELECT COUNT(n) FROM Node n WHERE n.domainId = :domainId AND n.groupId = :groupId AND n.processed = true")
    long countByDomainIdAndGroupIdAndProcessedTrue(@Param("domainId") UUID domainId, @Param("groupId") UUID groupId);
}