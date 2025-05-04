package com.shedule.x.scheduler;

import com.shedule.x.models.Domain;
import com.shedule.x.processors.MatchesCreationJobExecutor;
import com.shedule.x.repo.DomainRepository;
import com.shedule.x.repo.MatchingGroupRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;


@Profile("!test")
@Component
@Slf4j
@RequiredArgsConstructor
public class MatchesCreationScheduler {

    private final DomainRepository domainRepository;
    private final MatchingGroupRepository matchingGroupRepository;
    private final MatchesCreationJobExecutor matchesCreationJobExecutor;

    @Value("${match.cron-schedule}")
    private String cronSchedule;

    @Scheduled(cron = "${match.cron-schedule}")
    public void scheduledMatchingJob() {
        List<Domain> domains = domainRepository.findByIsActiveTrue();
        for (Domain domain : domains) {
            List<String> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            log.info("Starting matching for domain={} with groups: {}", domain.getName(), groupIds);
            for (String groupId : groupIds) {
                matchesCreationJobExecutor.processGroup(groupId, domain.getId());
            }
        }
    }
}
