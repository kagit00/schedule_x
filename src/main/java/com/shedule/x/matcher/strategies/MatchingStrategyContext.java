package com.shedule.x.matcher.strategies;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class MatchingStrategyContext {
    private final List<MatchingStrategy> strategies;

    public MatchingStrategy resolve(String mode) {
        return strategies.stream()
                .filter(strategy -> strategy.supports(mode))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported scheduling mode: " + mode));
    }
}