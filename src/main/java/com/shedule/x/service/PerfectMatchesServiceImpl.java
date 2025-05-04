package com.shedule.x.service;

import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.repo.PerfectMatchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class PerfectMatchesServiceImpl implements PerfectMatchesService {

    private final PerfectMatchRepository perfectMatchRepository;

    @Override
    public List<PerfectMatchEntity> fetchPerfectMatches(String groupId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        return perfectMatchRepository.findByGroupId(groupId, pageable).getContent();
    }
}
