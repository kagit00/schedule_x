package com.shedule.x.service;

import com.shedule.x.models.PerfectMatchEntity;

import java.util.List;

public interface PerfectMatchesService {
    List<PerfectMatchEntity> fetchPerfectMatches(String groupId, int page, int size);
}
