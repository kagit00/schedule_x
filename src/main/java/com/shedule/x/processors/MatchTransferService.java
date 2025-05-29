package com.shedule.x.processors;

import com.shedule.x.models.Domain;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
@Slf4j
public class MatchTransferService {
    private final MatchTransferProcessor matchTransferProcessor;

    @Async("matchesTransferExecutor")
    public void processGroup(String groupId, Domain domain) {
        log.info("Processing matches export for group for domain: {} {}", groupId, domain.getName());
        matchTransferProcessor.processMatchTransfer(groupId, domain);
    }
}
