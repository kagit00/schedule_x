package com.shedule.x.repo;

import com.shedule.x.models.NodesCursor;
import com.shedule.x.models.NodesCursorId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface NodesCursorRepository extends JpaRepository<NodesCursor, NodesCursorId> {

    Optional<NodesCursor> findByIdGroupIdAndIdDomainId(UUID groupId, UUID domainId);
}