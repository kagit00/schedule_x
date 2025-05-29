package com.shedule.x.repo;

import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Node;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import org.springframework.transaction.TransactionDefinition;
import io.micrometer.core.instrument.Tags;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


@Slf4j
@Repository
public class NodeRepositoryCustomImpl implements NodeRepositoryCustom {
    private static final String REPO_NAME = "NodeRepository";
    private final EntityManager entityManager;
    private final ExecutorService executorService;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;
    private final long futureTimeoutSeconds;

    public NodeRepositoryCustomImpl(
            EntityManager entityManager,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager,
            @Value("${node-fetch.thread-pool-size:#{T(java.lang.Runtime).getRuntime().availableProcessors()}}") int threadPoolSize,
            @Value("${node-fetch.queue-capacity:100}") int queueCapacity,
            @Value("${node-fetch.batch-size:1000}") int batchSize,
            @Value("${node-fetch.future-timeout-seconds:30}") long futureTimeoutSeconds
    ) {
        this.entityManager = entityManager;
        this.meterRegistry = meterRegistry;
        this.batchSize = batchSize;
        this.futureTimeoutSeconds = futureTimeoutSeconds;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setReadOnly(true);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.executorService = new ThreadPoolExecutor(
                threadPoolSize, threadPoolSize * 2, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                new ThreadFactoryBuilder().setNameFormat("node-repo-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Override
    public List<UUID> findIdsByGroupIdAndDomainId(String groupId, UUID domainId, Pageable pageable, LocalDateTime createdAfter) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            String jpql = buildIdQuery(createdAfter, pageable);
            TypedQuery<UUID> query = entityManager.createQuery(jpql, UUID.class)
                    .setParameter("groupId", groupId)
                    .setParameter("domainId", domainId)
                    .setHint("org.hibernate.cacheable", true)
                    .setHint("org.hibernate.fetchSize", batchSize); // Optimize fetch size
            if (createdAfter != null) {
                query.setParameter("createdAfter", createdAfter);
            }
            applyPagination(query, pageable);
            List<UUID> result = query.getResultList();
            log.info("Fetched {} node IDs for groupId={}", result.size(), groupId);
            meterRegistry.counter("node_ids_fetched_total", Tags.of("groupId", groupId)).increment(result.size());
            return result;
        } catch (Exception e) {
            meterRegistry.counter("node_fetch_ids_errors", Tags.of("groupId", groupId)).increment();
            log.error("Failed to fetch node IDs for groupId={}", groupId, e);
            throw new InternalServerErrorException("Failed to fetch node IDs");
        } finally {
            sample.stop(meterRegistry.timer("node_fetch_ids_duration", Tags.of("groupId", groupId)));
        }
    }

    @Override
    public CompletableFuture<List<Node>> findByIdsWithMetadataAsync(List<UUID> ids) {
        if (ids.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        Timer.Sample sample = Timer.start(meterRegistry);
        List<CompletableFuture<List<Node>>> futures = partition(ids, batchSize).stream()
                .map(batch -> CompletableFuture.supplyAsync(() ->
                                transactionTemplate.execute(status -> {
                                    TypedQuery<Node> query = entityManager.createQuery(
                                                    "SELECT n FROM Node n JOIN FETCH n.metaData WHERE n.id IN :ids", Node.class
                                            ).setParameter("ids", batch)
                                            .setHint("org.hibernate.cacheable", true)
                                            .setHint("org.hibernate.fetchSize", batchSize);
                                    return query.getResultList();
                                }), executorService)
                        .orTimeout(futureTimeoutSeconds, TimeUnit.SECONDS)
                        .exceptionally(throwable -> handleAsyncError("fetch nodes by IDs", batch.size(), throwable)))
                .toList();
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(futureTimeoutSeconds * 2, TimeUnit.SECONDS)
                .thenApply(v -> futures.stream().map(CompletableFuture::join).flatMap(List::stream).toList())
                .whenComplete((result, throwable) -> {
                    meterRegistry.counter("node_fetch_by_ids_total").increment(result.size());
                    sample.stop(meterRegistry.timer("node_fetch_by_ids_duration"));
                });
    }

    @Override
    @Transactional
    public void markAsProcessed(List<UUID> ids) {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            partition(ids, batchSize).forEach(batch -> entityManager.createQuery(
                    "UPDATE Node n SET n.processed = true WHERE n.id IN :ids"
            ).setParameter("ids", batch).executeUpdate());
            log.info("Marked {} nodes as processed", ids.size());
            meterRegistry.counter("node_mark_processed_total").increment(ids.size());
        } catch (Exception e) {
            meterRegistry.counter("node_mark_processed_errors").increment();
            log.error("Failed to mark {} nodes as processed", ids.size(), e);
            throw new InternalServerErrorException("Failed to mark nodes as processed");
        } finally {
            sample.stop(meterRegistry.timer("node_mark_processed_duration"));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Node>> findByGroupIdAsync(String groupId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        return executeAsyncQuery(
                "SELECT n FROM Node n JOIN FETCH n.metaData WHERE n.groupId = :groupId AND n.processed = false",
                query -> query.setParameter("groupId", groupId),
                "fetch nodes by groupId=" + groupId,
                Tags.of("groupId", groupId)
        ).thenApply(result -> (List<Node>) (List<?>) result)
                .whenComplete((result, throwable) ->
                        sample.stop(meterRegistry.timer("node_fetch_by_group_duration", Tags.of("groupId", groupId)))
                );
    }


    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<List<Node>> findFilteredByGroupIdAfterAsync(String groupId, LocalDateTime createdAfter) {
        Timer.Sample sample = Timer.start(meterRegistry);
        return executeAsyncQuery(
                "SELECT n FROM Node n JOIN FETCH n.metaData WHERE n.groupId = :groupId AND n.createdAt >= :createdAfter AND n.processed = false",
                query -> {
                    query.setParameter("groupId", groupId);
                    query.setParameter("createdAfter", createdAfter);
                },
                "fetch filtered nodes by groupId=" + groupId,
                Tags.of("groupId", groupId)
        ).thenApply(result -> (List<Node>) (List<?>) result)
                .whenComplete((result, throwable) ->
                sample.stop(meterRegistry.timer("node_fetch_filtered_duration", Tags.of("groupId", groupId))));
    }

    @Override
    public CompletableFuture<Set<String>> findDistinctMetadataKeysByGroupIdAsync(String groupId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        return executeAsyncQuery(
                "SELECT DISTINCT KEY(m) FROM Node n JOIN n.metaData m WHERE n.groupId = :groupId AND n.processed = false",
                query -> query.setParameter("groupId", groupId),
                "fetch metadata keys by groupId=" + groupId,
                Tags.of("groupId", groupId),
                String.class
        ).thenApply(list -> (Set<String>) new HashSet<>(list))
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        meterRegistry.counter("node_fetch_metadata_keys_total", Tags.of("groupId", groupId)).increment(result.size());
                    }
                    sample.stop(meterRegistry.timer("node_fetch_metadata_keys_duration", Tags.of("groupId", groupId)));
                });
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down {} executor service", REPO_NAME);
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("ExecutorService did not terminate within 10 seconds, forcing shutdown");
                executorService.shutdownNow();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.error("ExecutorService failed to terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private String buildIdQuery(LocalDateTime createdAfter, Pageable pageable) {
        StringBuilder jpql = new StringBuilder(
                "SELECT n.id FROM Node n WHERE n.groupId = :groupId AND n.domainId = :domainId AND n.processed = false"
        );
        if (createdAfter != null) {
            jpql.append(" AND n.createdAt >= :createdAfter");
        }
        if (!pageable.getSort().isSorted()) {
            jpql.append(" ORDER BY n.createdAt ASC, n.id ASC");
        }
        return jpql.toString();
    }

    private void applyPagination(TypedQuery<?> query, Pageable pageable) {
        if (pageable instanceof KeysetPageable keyset && keyset.getLastId() != null) {
            query.setParameter("lastId", keyset.getLastId());
            query.setParameter("lastCreatedAt", keyset.getLastCreatedAt());
            query.setFirstResult(0);
        } else {
            query.setFirstResult((int) pageable.getOffset());
        }
        query.setMaxResults(pageable.getPageSize());
    }

    private <T> CompletableFuture<List<T>> executeAsyncQuery(
            String jpql, Consumer<TypedQuery<T>> paramSetter, String operation, Tags tags
    ) {
        return executeAsyncQuery(jpql, paramSetter, operation, tags, (Class<T>) Object.class);
    }

    private <T> CompletableFuture<List<T>> executeAsyncQuery(
            String jpql, Consumer<TypedQuery<T>> paramSetter, String operation, Tags tags, Class<T> resultType
    ) {
        return CompletableFuture.supplyAsync(() ->
                        transactionTemplate.execute(status -> {
                            TypedQuery<T> query = entityManager.createQuery(jpql, resultType)
                                    .setHint("org.hibernate.cacheable", true)
                                    .setHint("org.hibernate.fetchSize", batchSize);
                            paramSetter.accept(query);
                            return query.getResultList();
                        }), executorService)
                .orTimeout(futureTimeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(throwable -> handleAsyncError(operation, 0, throwable));
    }

    private <T> T handleAsyncError(String operation, int batchSize, Throwable throwable) {
        meterRegistry.counter("node_" + operation.replace(" ", "_") + "_errors").increment();
        log.error("Failed to {} (batch size: {})", operation, batchSize, throwable);
        throw new InternalServerErrorException("Failed to " + operation);
    }

    private static <T> List<List<T>> partition(List<T> list, int size) {
        return IntStream.range(0, (list.size() + size - 1) / size)
                .mapToObj(i -> list.subList(i * size, Math.min((i + 1) * size, list.size())))
                .toList();
    }

    public interface KeysetPageable extends Pageable {
        UUID getLastId();
        LocalDateTime getLastCreatedAt();
    }
}