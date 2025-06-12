package com.shedule.x.processors;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.config.factory.SerializerContext;
import com.shedule.x.utils.basic.StringConcatUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class MatchesCreationFinalizer {
    private final LSHIndex lshIndex;

    public void finalize(boolean cycleCompleted) {
        clean(cycleCompleted);
    }

    private void clean(boolean cycleCompleted) {
        Runtime rt = Runtime.getRuntime();
        long usedBytes = rt.totalMemory() - rt.freeMemory();
        long maxBytes  = rt.maxMemory();
        double usedPct = usedBytes / (double) maxBytes;

        long usedMB = usedBytes / (1024 * 1024);
        long maxMB  = maxBytes  / (1024 * 1024);

        if (usedPct >= 0.90) {
            log.error(
                    "High heap usage detected before cleanup: {} MB used out of {} MB ({}%). " +
                            "This may lead to OutOfMemoryError if it climbs further.",
                    usedMB,
                    maxMB,
                    String.format("%.1f", usedPct * 100.0)
            );
        } else if (usedPct >= 0.75) {
            log.warn(
                    "Elevated heap usage before cleanup: {} MB used out of {} MB ({}%).",
                    usedMB,
                    maxMB,
                    String.format("%.1f", usedPct * 100.0)
            );
        } else {
            log.debug(
                    "Heap usage before cleanup: {} MB used out of {} MB ({}%).",
                    usedMB,
                    maxMB,
                    String.format("%.1f", usedPct * 100.0)
            );
        }

        QueueManagerImpl.removeAll();
        lshIndex.clean();
        if (cycleCompleted) {
            SerializerContext.remove();
        }
        System.gc();

        long usedAfterBytes = rt.totalMemory() - rt.freeMemory();
        long usedAfterMB    = usedAfterBytes / (1024 * 1024);
        double usedAfterPct = usedAfterBytes / (double) maxBytes;

        log.info(
                "Heap usage after cleanup: {} MB used out of {} MB ({}%).",
                usedAfterMB,
                maxMB,
                String.format("%.1f", usedAfterPct * 100.0)
        );
    }

}
