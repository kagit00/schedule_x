package com.shedule.x.service;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;

import java.util.concurrent.CompletableFuture;

public interface PotentialMatchService {
    /**
     * The function `matchByGroup` takes a `MatchingRequest`, page number, and cycle ID as input
     * parameters and returns a CompletableFuture containing the count of nodes matched by group.
     * 
     * @param request The `request` parameter is of type `MatchingRequest`, which likely contains
     * information needed for matching nodes by group. This could include criteria for matching,
     * filters, or other relevant data for the operation.
     * @param page The `page` parameter in the `matchByGroup` method represents the page number of
     * results to retrieve. It is used for pagination, allowing you to specify which page of results
     * you want to fetch. This parameter is typically an integer value that indicates the specific page
     * of results to return.
     * @param cycleId The `cycleId` parameter in the method `matchByGroup` is a String type parameter.
     * It seems to represent an identifier or reference to a specific cycle.
     * @return A CompletableFuture object that will eventually contain the count of nodes that match
     * the given criteria specified in the MatchingRequest object for the specified page and cycleId.
     */
    CompletableFuture<NodesCount> matchByGroup(MatchingRequest request, int page, String cycleId);
}