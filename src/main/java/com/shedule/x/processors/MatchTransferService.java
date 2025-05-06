package com.shedule.x.processors;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.models.Domain;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.service.ExportService;
import com.shedule.x.service.PerfectMatchesService;
import com.shedule.x.service.PotentialMatchesService;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.ResponseMakerUtility;
import com.shedule.x.utils.basic.StringConcatUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.stream.Stream;


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
