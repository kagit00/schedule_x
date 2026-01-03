package com.shedule.x.scheduler;


import com.shedule.x.models.Domain;
import com.shedule.x.processors.MatchTransferService;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.service.DomainService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;


@Component
@Profile("!test")
@Slf4j
@RequiredArgsConstructor
public class MatchesTransferScheduler {

    private final MatchTransferService matchTransferService;
    private final MatchingGroupRepository matchingGroupRepository;
    private final DomainService domainService;
    private final Executor matchTransferGroupExecutor;
    private final MeterRegistry meterRegistry;

    @Scheduled(cron = "0 38 2 * * *", zone = "Asia/Kolkata")
    public void scheduledMatchesTransferJob() {
        List<Domain> domains = domainService.getActiveDomains();
        if (domains.isEmpty()) {
            log.warn("No domains available for match transfer");
            return;
        }

        ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) matchTransferGroupExecutor;
        meterRegistry.gauge("group_executor_active_threads", executor, ThreadPoolTaskExecutor::getActiveCount);
        meterRegistry.gauge("group_executor_queue_size", executor, e -> {
            int size = e.getThreadPoolExecutor().getQueue().size();
            if (size > 40) {
                log.warn("Group executor queue depth high: {}", size);
                meterRegistry.counter("group_executor_queue_high", "threshold", "80_percent").increment();
            }
            return size;
        });

        for (Domain domain : domains) {
            List<UUID> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            log.info("Starting matching for domain={} with groups: {}", domain.getName(), groupIds);
            for (UUID groupId : groupIds) {
                Timer.Sample sample = Timer.start(meterRegistry);
                CompletableFuture.runAsync(() -> matchTransferService.processGroup(groupId, domain), matchTransferGroupExecutor)
                        .whenComplete((v, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to process group {} for domain {}", groupId, domain.getName(), throwable);
                                meterRegistry.counter("group_process_failed", "groupId", groupId.toString()).increment();
                            }
                            sample.stop(meterRegistry.timer("group_process_duration", "groupId", groupId.toString()));
                        });
            }
        }
    }
}