package com.shedule.x.service;

import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.repo.PotentialMatchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PotentialMatchesServiceImpl implements PotentialMatchesService {

    private final PotentialMatchRepository potentialMatchRepository;

    @Override
    public List<PotentialMatchEntity> fetchPotentialMatches(UUID groupId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        return potentialMatchRepository.findByGroupId(groupId, pageable).getContent();
    }
}
