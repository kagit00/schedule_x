package com.shedule.x.service;

import com.shedule.x.dto.MatchingRequest;
import java.util.concurrent.CompletableFuture;


public interface PerfectMatchService {
    CompletableFuture<Void> processAndSaveMatches(MatchingRequest request);
}