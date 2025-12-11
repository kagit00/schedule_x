package com.shedule.x.processors;

import com.shedule.x.dto.NodesTransferJobExchange;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.service.ImportJobService;
import com.shedule.x.service.PerfectMatchCreationService;
import com.shedule.x.utils.basic.BasicUtility;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;


@Slf4j
@Component
@RequiredArgsConstructor
public class ScheduleXPayloadProcessor {
    private final ImportJobService importJobService;

    public CompletableFuture<Void> processImportedNodesPayload(String payload) {
        if (payload == null || payload.isBlank()) {
            log.warn("Skipping processing: Blank payload");
            return CompletableFuture.completedFuture(null);
        }

        NodeExchange parsedPayload = BasicUtility.safeParse(payload, NodeExchange.class);
        if (parsedPayload == null) {
            log.warn("Failed to parse payload: {}", payload);
            return CompletableFuture.completedFuture(null);
        }

        return importJobService.startNodesImport(parsedPayload)
                .exceptionally(ex -> {
                    log.error("Error processing parsed payload for groupId={}", parsedPayload.getGroupId(), ex);
                    return null;
                });
    }

    public CompletableFuture<Void> processNodesImportJobStatusPayload(String payload) {
        if (payload == null || payload.isBlank()) {
            log.warn("Null or empty payload received");
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.supplyAsync(() -> {
            NodesTransferJobExchange job = BasicUtility.safeParse(payload, NodesTransferJobExchange.class);
            if (job == null) {
                log.warn("Failed to parse job status payload: {}", payload);
                return null;
            }

            log.info(
                    "Received job status: jobId={}, groupId={}, status={}, processedNodes={}, totalNodes={}, successCount={}, failedCount={}",
                    job.getJobId(),
                    job.getGroupId(),
                    job.getStatus(),
                    job.getProcessed(),
                    job.getTotal(),
                    job.getSuccessList() != null ? job.getSuccessList().size() : 0,
                    job.getFailedList() != null ? job.getFailedList().size() : 0
            );
            return null;
        }).handle((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to process job status payload: {}", payload, throwable);
            }
            return null;
        });
    }
}