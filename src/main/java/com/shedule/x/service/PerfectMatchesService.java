package com.shedule.x.service;

import com.shedule.x.models.PerfectMatchEntity;

import java.util.List;
import java.util.UUID;

public interface PerfectMatchesService {
    List<PerfectMatchEntity> fetchPerfectMatches(UUID groupId, int page, int size);
}
