package com.shedule.x.processors;

import com.shedule.x.models.Domain;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.Semaphore;

@Service
@RequiredArgsConstructor
@Slf4j
public class MatchTransferService {

    private final MatchTransferProcessor matchTransferProcessor;

    public void processGroup(UUID groupId, Domain domain) {
        matchTransferProcessor.processMatchTransfer(groupId, domain);
    }
}