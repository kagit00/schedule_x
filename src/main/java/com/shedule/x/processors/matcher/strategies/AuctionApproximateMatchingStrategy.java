package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.exceptions.BadRequestException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import java.util.*;

import com.shedule.x.service.GraphRecords.PotentialMatch;

import org.springframework.beans.factory.annotation.Value;

import java.util.stream.Collectors;


@Slf4j
@Component("auctionApproximateMatchingStrategy")
public class AuctionApproximateMatchingStrategy implements MatchingStrategy {
    private static final double MIN_BID_INCREMENT = 1e-6;
    private final int maxIterations;

    public AuctionApproximateMatchingStrategy(@Value("${matching.maxIterations:10000}") int maxIterations) {
        if (maxIterations < 1) {
            throw new BadRequestException("maxIterations must be a positive integer.");
        }
        this.maxIterations = maxIterations;
    }

    @Override
    public Map<String, List<MatchResult>> match(List<PotentialMatch> potentialMatches, UUID groupId, UUID domainId) {
        if (potentialMatches.isEmpty()) {
            log.error("Invalid input: potentialMatches is null for groupId: {}", groupId);
            return Collections.emptyMap();
        }
        if (Objects.isNull(groupId)) {
            log.warn("Input groupId is null. Only potential matches with null groupId will be considered.");
        }
        if (domainId == null) {
            log.warn("Input domainId is null. Only potential matches with null domainId will be considered.");
        }

        Map<String, List<PotentialMatch>> leftToMatches = new HashMap<>();
        Map<String, Double> prices = new HashMap<>();

        for (PotentialMatch pm : potentialMatches) {
            // Filter by groupId and domainId using Objects.equals for null-safe comparison
            boolean groupIdMatches = Objects.equals(groupId, pm.getGroupId());
            boolean domainIdMatches = Objects.equals(domainId, pm.getDomainId());

            if (!groupIdMatches || !domainIdMatches) {
                log.debug("Skipping PotentialMatch due to non-matching groupId ({}) or domainId ({}): {}", pm.getGroupId(), pm.getDomainId(), pm);
                continue;
            }

            // Existing validation for score and null fields
            if (pm.getReferenceId() == null || pm.getMatchedReferenceId() == null ||
                    Double.isNaN(pm.getCompatibilityScore()) || pm.getCompatibilityScore() < 0 || pm.getCompatibilityScore() > 1.0) {
                log.warn("Skipping invalid PotentialMatch (score out of [0, 1] or null fields) after group/domain filter: {} for groupId: {}", pm, groupId);
                continue;
            }
            leftToMatches.computeIfAbsent(pm.getReferenceId(), k -> new ArrayList<>()).add(pm);
            prices.putIfAbsent(pm.getMatchedReferenceId(), 0.0);
        }

        if (leftToMatches.isEmpty()) {
            log.info("No potential matches found after filtering by groupId: {} and domainId: {}", groupId, domainId);
            return Collections.emptyMap();
        }

        // Deduplicate and sort matches for each left node
        leftToMatches.replaceAll((leftId, matches) -> {
            Map<String, PotentialMatch> deduped = matches.stream()
                    .collect(Collectors.toMap(
                            PotentialMatch::getMatchedReferenceId,
                            pm -> pm,
                            (existing, replacement) -> existing.getCompatibilityScore() > replacement.getCompatibilityScore() ? existing : replacement,
                            LinkedHashMap::new // Maintain insertion order, though not strictly needed after sorting
                    ));
            List<PotentialMatch> sortedMatches = new ArrayList<>(deduped.values());
            sortedMatches.sort((a, b) -> {
                int cmp = Double.compare(b.getCompatibilityScore(), a.getCompatibilityScore());
                return cmp != 0 ? cmp : a.getMatchedReferenceId().compareTo(b.getMatchedReferenceId());
            });
            return sortedMatches;
        });

        Map<String, String> assignments = new HashMap<>();
        Map<String, String> reverseAssignments = new HashMap<>();
        Map<String, PotentialMatch> bestMatches = new HashMap<>();
        Set<String> unmatchedLefts = new HashSet<>(leftToMatches.keySet());

        int iteration = 0;

        while (!unmatchedLefts.isEmpty() && iteration < maxIterations) {
            iteration++;
            Set<String> currentIterationLosers = new HashSet<>();
            Iterator<String> iterator = unmatchedLefts.iterator();
            boolean progressMadeThisIteration = false;

            while (iterator.hasNext()) {
                String leftId = iterator.next();
                List<PotentialMatch> matches = leftToMatches.get(leftId); // get() is safe here as leftId comes from leftToMatches.keySet()

                PotentialMatch bestMatchForLeft = findBestMatch(matches, prices, reverseAssignments, leftId,
                        assignments, bestMatches, currentIterationLosers);

                if (bestMatchForLeft != null) {
                    progressMadeThisIteration = true;
                    iterator.remove();
                } else {
                    log.debug("LeftId: {} could not find a non-negative profit match in iteration {} for groupId: {}. Removing from auction.", leftId, iteration, groupId);
                    iterator.remove();
                }
            }

            unmatchedLefts.addAll(currentIterationLosers);

            if (!progressMadeThisIteration && currentIterationLosers.isEmpty()) {
                log.debug("No progress made in iteration {} and no new losers for groupId: {}. Breaking auction.", iteration, groupId);
                break;
            }

            if (iteration % 100 == 0) {
                log.info("Iteration {}: {} unmatched lefts for groupId: {}", iteration, unmatchedLefts.size(), groupId);
            }
        }

        if (iteration >= maxIterations) {
            log.warn("Reached max iterations ({}) for groupId: {}. Unmatched lefts remaining: {}", maxIterations, groupId, unmatchedLefts.size());
        }

        Set<String> trulyUnmatchedAfterAuction = new HashSet<>(unmatchedLefts);
        for (String leftId : trulyUnmatchedAfterAuction) {
            List<PotentialMatch> matches = leftToMatches.get(leftId); // get() is safe here

            PotentialMatch bestFallbackMatch = matches.stream()
                    .filter(pm -> !reverseAssignments.containsKey(pm.getMatchedReferenceId()))
                    .max(Comparator.comparingDouble(pm -> pm.getCompatibilityScore() - prices.getOrDefault(pm.getMatchedReferenceId(), 0.0)))
                    .orElse(null);

            if (bestFallbackMatch != null) {
                String rightId = bestFallbackMatch.getMatchedReferenceId();
                assignments.put(leftId, rightId);
                bestMatches.put(leftId, bestFallbackMatch);
                reverseAssignments.put(rightId, leftId);
                unmatchedLefts.remove(leftId);
                log.debug("Fallback match for leftId: {} to rightId: {} with score: {} for groupId: {}",
                        leftId, rightId, bestFallbackMatch.getCompatibilityScore(), groupId);
            } else {
                log.debug("LeftId: {} has candidates, but no available (unassigned) fallback match for groupId: {}", leftId, groupId);
            }
        }

        Set<String> allRightIds = prices.keySet();
        Set<String> assignedRightIds = new HashSet<>(reverseAssignments.keySet());
        Set<String> unassignedRights = allRightIds.stream()
                .filter(rightId -> !assignedRightIds.contains(rightId))
                .collect(Collectors.toSet());

        if (!unassignedRights.isEmpty()) {
            log.info("Unassigned rights for groupId: {}: {}. Count: {}", groupId, unassignedRights, unassignedRights.size());
        }

        if (!unmatchedLefts.isEmpty()) {
            log.info("Truly unmatched lefts after auction and fallback for groupId: {}: {}. Count: {}", groupId, unmatchedLefts, unmatchedLefts.size());
        }

        // Log how many rights ended up with a price > 0 for observability
        long rightsWithNonZeroPrice = prices.values().stream().filter(price -> price > 0.0).count();
        if (rightsWithNonZeroPrice > 0) {
            log.debug("Number of rights with non-zero prices after auction for groupId: {}: {}", groupId, rightsWithNonZeroPrice);
        }

        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : assignments.entrySet()) {
            String leftId = entry.getKey();
            String rightId = entry.getValue();
            if (rightId.equals(reverseAssignments.get(rightId))) {
                PotentialMatch match = bestMatches.get(leftId);
                if (match != null) {
                    result.put(leftId, Collections.singletonList(new MatchResult(rightId, match.getCompatibilityScore())));
                } else {
                    log.error("Consistency error: Matched leftId: {} to rightId: {} but no PotentialMatch found in bestMatches for groupId: {}",
                            leftId, rightId, groupId);
                }
            } else {
                log.error("Inconsistent assignment detected for leftId: {} to rightId: {}. Reverse assignment points to {} for groupId: {}",
                        leftId, rightId, reverseAssignments.get(rightId), groupId);
            }
        }

        log.info("Matching completed for groupId: {}. Matched {} left nodes.", groupId, result.size());
        return result;
    }

    private PotentialMatch findBestMatch(List<PotentialMatch> matches, Map<String, Double> prices,
                                         Map<String, String> reverseAssignments, String currentLeftId,
                                         Map<String, String> assignments, Map<String, PotentialMatch> bestMatches,
                                         Set<String> currentIterationLosers) {
        PotentialMatch bestMatch = null;
        double maxProfit = 0.0;

        for (PotentialMatch pm : matches) {
            String rightId = pm.getMatchedReferenceId();
            double compatibility = pm.getCompatibilityScore();
            double currentPrice = prices.getOrDefault(rightId, 0.0);

            double profit = compatibility - currentPrice;
            if (profit < 0 && bestMatch == null) {
                break;
            }

            if (profit > maxProfit) {
                maxProfit = profit;
                bestMatch = pm;
            } else if (profit == maxProfit) {
                if (bestMatch == null || pm.getMatchedReferenceId().compareTo(bestMatch.getMatchedReferenceId()) < 0) {
                    bestMatch = pm;
                }
            }
        }

        if (bestMatch != null && maxProfit >= 0) {
            String rightId = bestMatch.getMatchedReferenceId();
            String previousLeftId = reverseAssignments.get(rightId);

            if (previousLeftId != null && !previousLeftId.equals(currentLeftId)) {
                assignments.remove(previousLeftId);
                bestMatches.remove(previousLeftId);
                currentIterationLosers.add(previousLeftId);
                log.debug("Outbid: leftId {} lost rightId {} to currentLeftId {}", previousLeftId, rightId, currentLeftId);
            }

            double bidIncrement = Math.max(MIN_BID_INCREMENT, maxProfit * 0.01);
            prices.put(rightId, prices.getOrDefault(rightId, 0.0) + bidIncrement); // Defensive getOrDefault

            assignments.put(currentLeftId, rightId);
            bestMatches.put(currentLeftId, bestMatch);
            reverseAssignments.put(rightId, currentLeftId);
            return bestMatch;
        }
        return null;
    }

    @Override
    public boolean supports(String mode) {
        return "AuctionApproximateMatchingStrategy".equalsIgnoreCase(mode);
    }
}