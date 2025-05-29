package com.shedule.x.processors;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchSuggestionsExchange;
import com.shedule.x.dto.MatchTransfer;
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
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
@RequiredArgsConstructor
@Slf4j
public class MatchTransferProcessor {
    private static final String MATCH_EXPORT_TOPIC = "matches-suggestions";

    private final PotentialMatchesService potentialMatchesService;
    private final PerfectMatchesService perfectMatchesService;
    private final ExportService exportService;
    private final ScheduleXProducer scheduleXProducer;
    private static final int BATCH_SIZE = 5000;

    @Transactional
    public void processMatchTransfer(String groupId, Domain domain) {
        try {
            int batchIndex = 0;

            while (true) {
                List<PotentialMatchEntity> potentialBatch = potentialMatchesService.fetchPotentialMatches(groupId, batchIndex, BATCH_SIZE);
                List<PerfectMatchEntity> perfectBatch = perfectMatchesService.fetchPerfectMatches(groupId, batchIndex, BATCH_SIZE);

                if (potentialBatch.isEmpty() && perfectBatch.isEmpty()) {
                    log.info("No more matches to export for group '{}'. Total batches: {}", groupId, batchIndex);
                    break;
                }

                List<MatchTransfer> combinedBatch = Stream.concat(
                        potentialBatch.stream().map(ResponseMakerUtility::buildMatchTransfer),
                        perfectBatch.stream().map(ResponseMakerUtility::buildMatchTransfer)
                ).collect(Collectors.toList());


                ExportedFile file = exportService.exportMatchesAsCsv(combinedBatch, groupId, domain.getId());
                MatchSuggestionsExchange payload = GraphRequestFactory.buildFileReference(
                        groupId,
                        file.getFilePath(),
                        file.getFileName(),
                        file.getContentType(),
                        domain.getId());
                scheduleXProducer.sendMessage(
                        StringConcatUtil.concatWithSeparator("-", domain.getName().toLowerCase(), MATCH_EXPORT_TOPIC),
                        StringConcatUtil.concatWithSeparator("-", domain.getId().toString(), groupId),
                        BasicUtility.stringifyObject(payload),
                        false
                );

                log.info("Exported {} matches for group '{}', batch {} for domain: {}", combinedBatch.size(), groupId, batchIndex, domain.getName());
                batchIndex++;
            }
        } catch (Exception e) {
            log.error("Match export failed for group '{}'", groupId, e);
        }
    }
}
