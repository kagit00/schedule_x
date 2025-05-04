package com.shedule.x.repo;

import com.shedule.x.models.Domain;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface DomainRepository extends JpaRepository<Domain, UUID> {
    Domain findByName(String name);
    List<Domain>  findByIsActiveTrue();
}