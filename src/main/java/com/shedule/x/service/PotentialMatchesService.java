package com.shedule.x.service;

import com.shedule.x.models.PotentialMatchEntity;

import java.util.List;
import java.util.UUID;

public interface PotentialMatchesService {
    List<PotentialMatchEntity> fetchPotentialMatches(UUID groupId, int page, int size);
}
