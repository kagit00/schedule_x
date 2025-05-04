package com.shedule.x.repo;

import com.shedule.x.models.Edge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.UUID;

@Repository
public interface EdgeRepository extends JpaRepository<Edge, UUID> {
}


