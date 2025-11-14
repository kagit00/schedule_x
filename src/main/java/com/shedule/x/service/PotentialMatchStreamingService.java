package com.shedule.x.service;

import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.utils.db.QueryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import javax.sql.DataSource;

import static com.shedule.x.utils.monitoring.MemoryMonitoringUtility.logMemoryUsage;

@Slf4j
@Service
@RequiredArgsConstructor
public class PotentialMatchStreamingService {

    private final DataSource dataSource;
    @PersistenceContext
    private EntityManager entityManager;
    @Value("${matching.max.memory.mb:1024}") private long maxMemoryMb;

    public void streamAllMatches(
            UUID groupId,
            UUID domainId,
            Consumer<List<PotentialMatchEntity>> batchConsumer,
            int batchSize
    ) {
        final int maxRetries = 3;
        final int memorySafeBatchSize = Math.min(batchSize, 1000);
        int retryCount = 0;

        while (retryCount <= maxRetries) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         QueryUtils.getAllPotentialMatchesStreamingSQL(),
                         ResultSet.TYPE_FORWARD_ONLY,
                         ResultSet.CONCUR_READ_ONLY)) {

                conn.setAutoCommit(false);
                ps.setFetchSize(memorySafeBatchSize);

                ps.setObject(1, groupId);
                ps.setObject(2, domainId);

                try (ResultSet rs = ps.executeQuery()) {

                    List<PotentialMatchEntity> buffer = new ArrayList<>(memorySafeBatchSize);
                    int totalRecords = 0;
                    long lastMemoryLog = System.currentTimeMillis();

                    while (rs.next()) {
                        String referenceId = rs.getString("reference_id");
                        String matchedReferenceId = rs.getString("matched_reference_id");

                        if (referenceId == null || matchedReferenceId == null) {
                            log.warn("Skipping row with null reference_id or matched_reference_id for groupId={}, domainId={}",
                                    groupId, domainId);
                            continue;
                        }

                        PotentialMatchEntity entity = new PotentialMatchEntity();
                        entity.setReferenceId(referenceId);
                        entity.setMatchedReferenceId(matchedReferenceId);
                        entity.setCompatibilityScore(rs.getDouble("compatibility_score"));
                        entity.setGroupId(UUID.fromString(rs.getString("group_id")));
                        entity.setDomainId(UUID.fromString(rs.getString("domain_id")));
                        buffer.add(entity);
                        totalRecords++;

                        // Flush when buffer reaches safe size
                        if (buffer.size() >= memorySafeBatchSize) {
                            log.debug("Streaming batch of {} records for groupId={}, domainId={}, total={}",
                                    buffer.size(), groupId, domainId, totalRecords);
                            batchConsumer.accept(buffer);
                            buffer = new ArrayList<>(memorySafeBatchSize);

                            long currentTime = System.currentTimeMillis();
                            if (currentTime - lastMemoryLog > 30000) {
                                logMemoryUsage("Potential_Matches_Streaming", groupId, maxMemoryMb);
                                lastMemoryLog = currentTime;

                                // Suggest GC if processing large datasets
                                if (totalRecords > 100000) {
                                    System.gc();
                                }
                            }
                        }
                    }

                    // Final batch
                    if (!buffer.isEmpty()) {
                        log.info("Streaming final batch of {} records for groupId={}, domainId={}",
                                buffer.size(), groupId, domainId);
                        batchConsumer.accept(buffer);
                    }

                    log.info("Total records streamed: {} for groupId={}, domainId={}",
                            totalRecords, groupId, domainId);

                    conn.commit();
                    return;

                }

            } catch (SQLException e) {
                retryCount++;
                log.error("Streaming failed on attempt {}/{} for groupId={}, domainId={}: {}",
                        retryCount, maxRetries, groupId, domainId, e.getMessage(), e);
                if (retryCount > maxRetries) {
                    throw new RuntimeException("Error streaming potential matches after " + maxRetries + " retries", e);
                }

                log.warn("Retrying streaming attempt {}/{} for groupId={}, domainId={}",
                        retryCount, maxRetries, groupId, domainId);

                try {
                    Thread.sleep(1000L * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry delay", ie);
                }
            }
        }
    }


    @Transactional
    public Stream<PotentialMatchEntity> streamMatches(UUID groupId, UUID domainId, int offset, int limit) {
        return entityManager.createQuery(QueryUtils.getPotentialMatchesStreamingSQL(), PotentialMatchEntity.class)
                .setParameter("groupId", groupId)
                .setParameter("domainId", domainId)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .getResultStream();
    }
}