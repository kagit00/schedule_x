package com.shedule.x.service;

import com.shedule.x.models.PotentialMatchEntity;

import java.util.List;

public interface PotentialMatchesService {
    List<PotentialMatchEntity> fetchPotentialMatches(String groupId, int page, int size);
}
