package com.shedule.x.scheduler;


import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.models.Domain;
import com.shedule.x.processors.MatchTransferService;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.service.DomainService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;


@Profile("!test")
@Component
@Slf4j
@RequiredArgsConstructor
public class MatchesTransferScheduler {

    private final MatchTransferService matchTransferService;
    private final MatchingGroupRepository matchingGroupRepository;
    private final DomainService domainService;


    @Scheduled(cron = "${match.transfer.cron-schedule}")
    public void scheduledMatchesTransferJob() {
        List<Domain> domains = domainService.getActiveDomains();
        if (domains.isEmpty()) throw new BadRequestException("No domains available");

        for (Domain domain : domains) {
            List<String> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            log.info("Starting matching for domain={} with groups: {}", domain.getName(), groupIds);
            for (String groupId : groupIds) {
                matchTransferService.processGroup(groupId, domain);
            }
        }
    }
}
