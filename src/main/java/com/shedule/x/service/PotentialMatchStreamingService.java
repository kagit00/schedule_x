package com.shedule.x.service;

import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.utils.db.QueryUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
@Service
@RequiredArgsConstructor
public class PotentialMatchStreamingService {

    private final DataSource dataSource;

    public void streamAllMatches(
            UUID groupId,
            UUID domainId,
            Consumer<List<PotentialMatchEntity>> batchConsumer,
            int batchSize
    ) {
        final int maxRetries = 3;
        int retryCount = 0;

        while (retryCount <= maxRetries) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         QueryUtils.getAllPotentialMatchesStreamingSQL(),
                         ResultSet.TYPE_FORWARD_ONLY,
                         ResultSet.CONCUR_READ_ONLY)) {

                conn.setAutoCommit(false);
                ps.setFetchSize(batchSize);

                ps.setObject(1, groupId);
                ps.setObject(2, domainId);

                try (ResultSet rs = ps.executeQuery()) {

                    List<PotentialMatchEntity> buffer = new ArrayList<>(batchSize);
                    int totalRecords = 0;

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

                        if (buffer.size() == batchSize) {
                            log.info("Streaming batch of {} records for groupId={}, domainId={}",
                                    buffer.size(), groupId, domainId);
                            batchConsumer.accept(buffer);
                            buffer = new ArrayList<>(batchSize);
                        }
                    }

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

    @PersistenceContext
    private EntityManager entityManager;


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