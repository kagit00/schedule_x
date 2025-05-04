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
public class MatchTransferProcessor {
    private static final String MATCH_EXPORT_TOPIC = "-match-suggestions";

    private final PotentialMatchesService potentialMatchesService;
    private final PerfectMatchesService perfectMatchesService;
    private final ExportService exportService;
    private final ScheduleXProducer scheduleXProducer;

    @Value("${export.format}")
    private String matchExportFormat;

    @Value("${export.batch-size}")
    private int batchSize;


    @Async("matchesTransferExecutor")
    public void processGroup(String groupId, Domain domain) {
        log.info("Processing matches export for group for domain: {} {}", groupId, domain.getName());

        try {
            int batchIndex = 0;

            while (true) {
                List<PotentialMatchEntity> potentialBatch = potentialMatchesService.fetchPotentialMatches(groupId, batchIndex, batchSize);
                List<PerfectMatchEntity> perfectBatch = perfectMatchesService.fetchPerfectMatches(groupId, batchIndex, batchSize);

                if (potentialBatch.isEmpty() && perfectBatch.isEmpty()) {
                    log.info("No more matches to export for group '{}'. Total batches: {}", groupId, batchIndex);
                    break;
                }

                List<MatchTransfer> combinedBatch = Stream.concat(
                        potentialBatch.stream().map(ResponseMakerUtility::buildMatchTransfer),
                        perfectBatch.stream().map(ResponseMakerUtility::buildMatchTransfer)
                ).toList();

                String batchFileName = String.format("%s_matches_batch_%d", groupId, batchIndex);
                ExportedFile file = new ExportedFile(new byte[0], batchFileName, "text/plain", groupId, domain.getId());

                if ("csv".equalsIgnoreCase(matchExportFormat)) {
                    file = exportService.exportMatchesAsCsv(combinedBatch, groupId, domain.getId());
                }

                NodeExchange payload = GraphRequestFactory.build(groupId, file.content(), file.fileName(), file.contentType(), domain.getId());
                scheduleXProducer.sendMessage(
                        StringConcatUtil.concat(domain.getName().toLowerCase(), MATCH_EXPORT_TOPIC),
                        StringConcatUtil.concatWithSeparator("-", domain.getId().toString(), groupId),
                        BasicUtility.stringifyObject(payload)
                );

                log.info("Exported {} matches for group '{}', batch {} for domain: {}", combinedBatch.size(), groupId, batchIndex, domain.getName());
                batchIndex++;
            }
        } catch (Exception e) {
            log.error("Match export failed for group '{}'", groupId, e);
        }
    }
}
