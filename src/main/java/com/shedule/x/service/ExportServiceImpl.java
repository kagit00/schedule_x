package com.shedule.x.service;

import com.shedule.x.config.ExportConfig;
import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import com.shedule.x.utils.basic.StringConcatUtil;
import com.shedule.x.utils.media.praquet.GenericParquetWriter;
import com.shedule.x.utils.media.praquet.ParquetFieldExtractor;
import com.shedule.x.utils.media.praquet.ParquetSchemaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
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

@Slf4j
@Service
@RequiredArgsConstructor
public class ExportServiceImpl implements ExportService {

    private final MeterRegistry meterRegistry;
    private final ExportConfig exportConfig;
    private final Semaphore semaphore;
    private final List<ParquetFieldExtractor<MatchTransfer>> matchFieldExtractors;
    private final MinioUploadService minioUploadService;
    private final Environment env;

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
        boolean useMinio = Boolean.parseBoolean(env.getProperty("minio.use"));

        if (useMinio) {
            return writeParquetFileToMinio(matchesSupplier, fileName, groupId, domainId);
        } else {
            return writeParquetFileToLocal(matchesSupplier, fileName, groupId, domainId);
        }
    }

    private ExportedFile writeParquetFileToLocal(Supplier<Stream<MatchTransfer>> matchesSupplier, String fileName, String groupId, UUID domainId) {
        String endpoint = env.getProperty("minio.endpoint");
        String bucket = env.getProperty("minio.matches.export.bucket-name");
        exportConfig.setBaseDir(StringConcatUtil.concat(endpoint, "/", bucket));
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
                        (match, s) -> {
                            recordCount.incrementAndGet();
                            return ParquetSchemaUtil.<MatchTransfer>createRecord(match, groupId, s, matchFieldExtractors);
                        }
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

    private ExportedFile writeParquetFileToMinio(Supplier<Stream<MatchTransfer>> matchesSupplier, String fileName, String groupId, UUID domainId) {
        String objectName = domainId.toString() + "/" + groupId + "/" + fileName;
        String bucketName = env.getProperty("minio.matches.export.bucket-name");
        String endpoint = env.getProperty("minio.endpoint");

        for (int attempt = 1; attempt <= exportConfig.getRetryCount(); attempt++) {
            try {
                // Create a temporary file
                Path tempFile = Files.createTempFile("matches_", ".parquet");
                AtomicLong recordCount = new AtomicLong(0);
                Schema schema = ParquetSchemaUtil.<MatchTransfer>getSchema(MatchTransfer.class, matchFieldExtractors);

                // Write to temporary file
                GenericParquetWriter.writeParquetFile(
                        exportConfig,
                        tempFile,
                        matchesSupplier.get(),
                        schema,
                        (match, s) -> {
                            recordCount.incrementAndGet();
                            return ParquetSchemaUtil.<MatchTransfer>createRecord(match, groupId, s, matchFieldExtractors);
                        }
                );

                // Upload to MinIO
                minioUploadService.upload(tempFile.toString(), objectName);

                // Clean up temp file
                Files.delete(tempFile);

                log.info("Exported {} match rows to MinIO: {}/{} for group {}, domain {} (attempt {})",
                        recordCount.get(), bucketName, objectName, groupId, domainId, attempt);
                meterRegistry.counter("export.parquet.success", "groupId", groupId, "domainId", domainId.toString()).increment();
                meterRegistry.counter("export.parquet.records", "groupId", groupId, "domainId", domainId.toString()).increment(recordCount.get());

                String minioUrl;
                if (endpoint != null) {
                    minioUrl = URI.create(endpoint)
                            .resolve("/" + bucketName + "/" + objectName)
                            .toString();
                } else throw new BadRequestException("Minio url missing in app.prop");

                return new ExportedFile(
                        minioUrl.getBytes(),
                        fileName,
                        "application/parquet",
                        groupId,
                        domainId,
                        minioUrl
                );

            } catch (Exception e) {
                log.error("Error writing Parquet file to MinIO for group {}, domain {}, object {}, attempt {}",
                        groupId, domainId, objectName, attempt, e);

                if (attempt == exportConfig.getRetryCount()) {
                    meterRegistry.counter("export.parquet.failure", "groupId", groupId, "domainId", domainId.toString()).increment();
                    throw new InternalServerErrorException(
                            String.format("Failed to export to MinIO for group %s, domain %s, object %s after %d attempts: %s",
                                    groupId, domainId, objectName, exportConfig.getRetryCount(), e.getMessage()));
                }

                try {
                    Thread.sleep(1000L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new InternalServerErrorException(
                            String.format("Interrupted during retry for group %s, domain %s, object %s: %s",
                                    groupId, domainId, objectName, ie.getMessage()));
                }
            }
        }

        throw new InternalServerErrorException(
                String.format("Unexpected failure after retries for group %s, domain %s, object %s",
                        groupId, domainId, domainId + "/" + groupId + "/" + fileName));
    }
}