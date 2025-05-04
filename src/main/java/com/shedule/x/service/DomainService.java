package com.shedule.x.service;

import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.models.Domain;
import com.shedule.x.repo.DomainRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DomainService {

    private final DomainRepository domainRepository;

    @Cacheable(value = "domainCache", key = "#domainId")
    public Domain getDomainById(UUID domainId) {
        return domainRepository.findById(domainId).orElseThrow(
                () -> new BadRequestException("Domain does not exist")
        );
    }

    @Cacheable(value = "domainsCache")
    public List<Domain> getActiveDomains() {
        return domainRepository.findByIsActiveTrue();
    }
}
