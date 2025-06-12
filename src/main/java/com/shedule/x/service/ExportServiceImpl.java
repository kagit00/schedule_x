package com.shedule.x.service;

import com.shedule.x.config.ExportConfig;
import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import com.shedule.x.utils.media.praquet.GenericParquetWriter;
import com.shedule.x.utils.media.praquet.ParquetFieldExtractor;
import com.shedule.x.utils.media.praquet.ParquetSchemaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;


@Service
@Slf4j
@RequiredArgsConstructor
public class ExportServiceImpl implements ExportService {

    private final MeterRegistry meterRegistry;
    private final ExportConfig exportConfig;
    private final Semaphore semaphore;
    private final List<ParquetFieldExtractor<MatchTransfer>> matchFieldExtractors;

    @Override
    public CompletableFuture<ExportedFile> exportMatches(Supplier<Stream<MatchTransfer>> matchesSupplier, String groupId, UUID domainId) {
        return CompletableFuture.supplyAsync(() -> {
            Timer.Sample timer = Timer.start(meterRegistry);
            try {
                semaphore.acquire();
                log.info("Acquired semaphore for group {}, domain {}, permits available: {}",
                        groupId, domainId, semaphore.availablePermits());
                return writeParquetFile(matchesSupplier, groupId, domainId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while acquiring semaphore for group {}, domain {}", groupId, domainId, e);
                throw new InternalServerErrorException(
                        String.format("Interrupted during export for group %s, domain %s: %s",
                                groupId, domainId, e.getMessage()));
            } finally {
                semaphore.release();
                log.info("Released semaphore for group {}, domain {}, permits available: {}",
                        groupId, domainId, semaphore.availablePermits());
                timer.stop(meterRegistry.timer("export.parquet.duration", "groupId", groupId, "domainId", domainId.toString()));
            }
        });
    }

    private ExportedFile writeParquetFile(Supplier<Stream<MatchTransfer>> matchesSupplier, String groupId, UUID domainId) {
        String fileName = groupId + "_matches_" + DefaultValuesPopulator.getUid() + ".parquet";
        java.nio.file.Path fullPath = Paths.get(exportConfig.getBaseDir(), domainId.toString(), groupId, fileName);

        for (int attempt = 1; attempt <= exportConfig.getRetryCount(); attempt++) {
            try {
                Files.createDirectories(fullPath.getParent());
                AtomicLong recordCount = new AtomicLong(0);
                Schema schema = ParquetSchemaUtil.<MatchTransfer>getSchema(MatchTransfer.class, matchFieldExtractors);

                GenericParquetWriter.writeParquetFile(
                        exportConfig,
                        fullPath,
                        matchesSupplier.get(),
                        schema,
                        (match, s) -> ParquetSchemaUtil.<MatchTransfer>createRecord(match, groupId, s, matchFieldExtractors)
                );

                log.info("Exported {} match rows to Parquet file: {} for group {}, domain {} (attempt {})",
                        recordCount.get(), fullPath, groupId, domainId, attempt);
                meterRegistry.counter("export.parquet.success", "groupId", groupId, "domainId", domainId.toString()).increment();
                meterRegistry.counter("export.parquet.records", "groupId", groupId, "domainId", domainId.toString()).increment(recordCount.get());
                return new ExportedFile(null, fileName, "application/parquet", groupId, domainId, fullPath.toString());

            } catch (IOException e) {
                log.error("Error writing Parquet file for group {}, domain {}, path {}, attempt {}",
                        groupId, domainId, fullPath, attempt, e);

                if (attempt == exportConfig.getRetryCount()) {
                    meterRegistry.counter("export.parquet.failure", "groupId", groupId, "domainId", domainId.toString()).increment();
                    throw new InternalServerErrorException(
                            String.format("Failed to export to Parquet for group %s, domain %s, path %s after %d attempts: %s",
                                    groupId, domainId, fullPath, exportConfig.getRetryCount(), e.getMessage()));
                }

                try {
                    Thread.sleep(1000L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new InternalServerErrorException(
                            String.format("Interrupted during retry for group %s, domain %s, path %s: %s",
                                    groupId, domainId, fullPath, ie.getMessage()));
                }
            }
        }

        throw new InternalServerErrorException(
                String.format("Unexpected failure after retries for group %s, domain %s, path %s",
                        groupId, domainId, Paths.get(exportConfig.getBaseDir(), domainId.toString(), groupId)));
    }
}