package com.shedule.x.processors;

import com.shedule.x.dto.NodesTransferJobExchange;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.service.ImportJobService;
import com.shedule.x.utils.basic.BasicUtility;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Slf4j
@Component
@RequiredArgsConstructor
public class ScheduleXPayloadProcessor {

    private final ImportJobService importJobService;

    public void processImportedNodesPayload(String payload) {
        NodeExchange parsedPayload = BasicUtility.safeParse(payload, NodeExchange.class);
        if (Objects.isNull(parsedPayload)) return;
        importJobService.startNodesImport(parsedPayload);
    }

    public void processNodesImportJobStatusPayload(String payload) {
        NodesTransferJobExchange job = BasicUtility.safeParse(payload, NodesTransferJobExchange.class);
        if (job == null) return;

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
    }
}

