package com.shedule.x.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.basic.BasicUtility;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import java.time.Duration;


@Slf4j
@Component
public class MatchCacheImpl implements MatchCache {
    private final RedisTemplate<String, byte[]> redisTemplate;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;
    private final ExecutorService cacheExecutor;
    private volatile boolean shutdownInitiated = false;
    private final long cacheTimeoutSeconds;
    private final int batchSize;

    public MatchCacheImpl(
            RedisTemplate<String, byte[]> redisTemplate,
            MeterRegistry meterRegistry,
            @Qualifier("redisObjectMapper") ObjectMapper objectMapper,
            @Qualifier("cacheExecutor") ExecutorService cacheExecutor,
            @Value("${match.cache.timeout-seconds:600}") long cacheTimeoutSeconds,
            @Value("${match.cache.batch-size:2000}") int batchSize
    ) {
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
        this.cacheExecutor = cacheExecutor;
        this.cacheTimeoutSeconds = cacheTimeoutSeconds;
        this.batchSize = batchSize;
    }

    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;
        log.info("MatchCacheImpl shutdown initiated");
    }

    @Retry(name = "matchCache")
    @CircuitBreaker(name = "matchCache", fallbackMethod = "cacheMatchesFallback")
    @Override
    public void cacheMatches(List<GraphRecords.PotentialMatch> matches, UUID groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Match caching aborted for groupId={}, processingCycleId={} due to shutdown", groupId, processingCycleId);
            throw new IllegalStateException("MatchCache is shutting down");
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        String zsetKey = String.format("matches:%s:%s:%s", domainId, groupId, processingCycleId);
        String keySet = String.format("match_keys:%s:%s", domainId, groupId);
        try {
            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            CompletableFuture.supplyAsync(() -> {
                redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                    for (int i = 0; i < matches.size(); i += batchSize) {
                        List<GraphRecords.PotentialMatch> batch = matches.subList(i, Math.min(i + batchSize, matches.size()));
                        log.info("Caching batch of {} matches for groupId={}, processingCycleId={}", batch.size(), groupId, processingCycleId);
                        for (GraphRecords.PotentialMatch match : batch) {
                            try {
                                byte[] serialized = serialize(match);
                                redisTemplate.opsForZSet().add(zsetKey, serialized, match.getCompatibilityScore());
                            } catch (Exception e) {
                                log.warn("Failed to serialize match for groupId={}, domainId={}, processingCycleId={}, skipping: {}",
                                        groupId, domainId, processingCycleId, e.getMessage());
                                meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                                        "processingCycleId", processingCycleId, "error_type", "serialization").increment();
                            }
                        }
                    }
                    redisTemplate.opsForSet().add(keySet, zsetKey.getBytes());
                    return null;
                });
                return null;
            }, cacheExecutor).thenAcceptAsync(v -> {
                Boolean expireSuccess = redisTemplate.expire(zsetKey, Duration.ofSeconds(cacheTimeoutSeconds));
                if (!Boolean.TRUE.equals(expireSuccess)) {
                    log.error("Failed to set expiration for key={} for groupId={}, processingCycleId={}", zsetKey, groupId, processingCycleId);
                    meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "error_type", "expire").increment();
                    resultFuture.completeExceptionally(new InternalServerErrorException("Failed to set expiration for cache key"));
                    return;
                }
                expireSuccess = redisTemplate.expire(keySet, Duration.ofSeconds(cacheTimeoutSeconds));
                if (!Boolean.TRUE.equals(expireSuccess)) {
                    log.error("Failed to set expiration for keySet={} for groupId={}, processingCycleId={}", keySet, groupId, processingCycleId);
                    meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "error_type", "expire").increment();
                    resultFuture.completeExceptionally(new InternalServerErrorException("Failed to set expiration for key set"));
                    return;
                }
                resultFuture.complete(null);
            }, cacheExecutor).exceptionally(t -> {
                meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                        "processingCycleId", processingCycleId, "error_type", "cache").increment();
                log.error("Failed to cache matches for groupId={}, domainId={}, processingCycleId={}: {}.", groupId, domainId, processingCycleId, t.getMessage());
                resultFuture.completeExceptionally(new CompletionException("Failed to cache matches", t));
                return null;
            });

            resultFuture.get(cacheTimeoutSeconds, TimeUnit.SECONDS);
            meterRegistry.counter("match_cache_total", "groupId", groupId.toString(), "domainId", domainId.toString(),
                    "processingCycleId", processingCycleId).increment(matches.size());
            log.info("Cached {} matches to key={} for groupId={}, domainId={}, processingCycleId={}",
                    matches.size(), zsetKey, groupId, domainId, processingCycleId);
        } catch (Exception e) {
            meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                    "processingCycleId", processingCycleId, "error_type", "cache").increment();
            log.error("Failed to cache matches for groupId={}, domainId={}, processingCycleId={}: {}",
                    groupId, domainId, processingCycleId, e.getMessage());
            throw new CompletionException("Failed to cache matches", e);
        } finally {
            sample.stop(meterRegistry.timer("match_cache_duration", "groupId", groupId.toString(), "domainId", domainId.toString(),
                    "processingCycleId", processingCycleId));
        }
    }

    @Retry(name = "matchCache")
    @CircuitBreaker(name = "matchCache", fallbackMethod = "streamEdgesFallback")
    @Override
    public AutoCloseableStream<Edge> streamEdges(UUID groupId, UUID domainId, String processingCycleId, int topK) {
        if (shutdownInitiated) {
            log.warn("Edge streaming aborted for groupId={}, processingCycleId={} due to shutdown", groupId, processingCycleId);
            return new AutoCloseableStream<>(Stream.empty());
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        String zsetKey = String.format("matches:%s:%s:%s", domainId, groupId, processingCycleId);
        try {
            CompletableFuture<Set<byte[]>> resultFuture = new CompletableFuture<>();
            CompletableFuture.supplyAsync(() ->
                    redisTemplate.opsForZSet().range(zsetKey, 0, topK - 1), cacheExecutor
            ).thenAcceptAsync(matches -> {
                log.info("Found {} matches for key={} in streamEdges for groupId={}, domainId={}, processingCycleId={}",
                        matches != null ? matches.size() : 0, zsetKey, groupId, domainId, processingCycleId);
                resultFuture.complete(matches);
            }, cacheExecutor).exceptionally(t -> {
                meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                        "processingCycleId", processingCycleId, "error_type", "stream").increment();
                log.error("Failed to stream edges for groupId={}, domainId={}, processingCycleId={}: {}",
                        groupId, domainId, processingCycleId, t.getMessage());
                resultFuture.completeExceptionally(t);
                return null;
            });

            Set<byte[]> matches = resultFuture.get(30, TimeUnit.SECONDS);
            if (matches == null || matches.isEmpty()) {
                log.warn("No matches found for key={} in streamEdges for groupId={}, domainId={}, processingCycleId={}",
                        zsetKey, groupId, domainId, processingCycleId);
                return new AutoCloseableStream<>(Stream.empty());
            }
            Stream<Edge> edgeStream = matches.stream()
                    .map(payload -> BasicUtility.deserializeToBytes(payload, GraphRecords.PotentialMatch.class))
                    .filter(Objects::nonNull)
                    .map(this::toEdge)
                    .peek(edge -> meterRegistry.counter("match_cache_streamed", "groupId", groupId.toString(), "domainId", domainId.toString(),
                            "processingCycleId", processingCycleId).increment());
            return new AutoCloseableStream<>(edgeStream.onClose(() -> sample.stop(meterRegistry.timer(
                    "match_cache_stream_duration", "groupId", groupId.toString(), "domainId", domainId.toString(),
                    "processingCycleId", processingCycleId))));
        } catch (Exception e) {
            meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                    "processingCycleId", processingCycleId, "error_type", "stream").increment();
            log.error("Failed to stream edges for groupId={}, domainId={}, processingCycleId={}: {}.",
                    groupId, domainId, processingCycleId, e.getMessage());
            throw new CompletionException("Failed to stream edges", e);
        }
    }

    @Retry(name = "matchCache")
    @CircuitBreaker(name = "matchCache", fallbackMethod = "getCachedMatchKeysFallback")
    @Override
    public Set<String> getCachedMatchKeysForDomainAndGroup(UUID groupId, UUID domainId) {
        if (shutdownInitiated) {
            log.warn("Key retrieval aborted for groupId={} due to shutdown", groupId);
            return Collections.emptySet();
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        String keySet = String.format("match_keys:%s:%s", domainId, groupId);
        try {
            CompletableFuture<Set<String>> resultFuture = new CompletableFuture<>();
            CompletableFuture.supplyAsync(() ->
                    redisTemplate.opsForSet().members(keySet), cacheExecutor
            ).thenAcceptAsync(keys -> {
                Set<String> result = keys != null ? keys.stream().map(String::new).collect(Collectors.toSet()) : Collections.emptySet();
                meterRegistry.counter("match_cache_keys_retrieved", "groupId", groupId.toString(), "domainId", domainId.toString()).increment(result.size());
                log.debug("Retrieved {} cached match keys for groupId={}, domainId={}", result.size(), groupId, domainId);
                resultFuture.complete(result);
            }, cacheExecutor).exceptionally(t -> {
                meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                        "error_type", "keys").increment();
                log.error("-Failed to retrieve cached match keys for groupId={}, domainId={}: {}", groupId, domainId, t.getMessage());
                resultFuture.completeExceptionally(t);
                return null;
            });

            return resultFuture.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                    "error_type", "keys").increment();
            log.error("Failed to retrieve cached match keys for groupId={}, domainId={}: {}", groupId, domainId, e.getMessage());
            throw new CompletionException("Failed to retrieve cached match keys", e);
        } finally {
            sample.stop(meterRegistry.timer("match_cache_keys_duration", "groupId", groupId.toString(), "domainId", domainId.toString()));
        }
    }

    @Retry(name = "matchCache")
    @CircuitBreaker(name = "matchCache", fallbackMethod = "clearMatchesFallback")
    @Override
    public void clearMatches(UUID groupId) {
        if (shutdownInitiated) {
            log.warn("Match clearing aborted for groupId={} due to shutdown", groupId);
            throw new IllegalStateException("MatchCache is shutting down");
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        String pattern = String.format("match_keys:*:%s", groupId);
        try {
            CompletableFuture<List<String>> keyFuture = new CompletableFuture<>();
            CompletableFuture.supplyAsync(() -> scanKeys(pattern), cacheExecutor)
                    .thenAcceptAsync(keyFuture::complete, cacheExecutor)
                    .exceptionally(t -> {
                        meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "error_type", "clear").increment();
                        log.error("Failed to scan keys for groupId={}: {}", groupId, t.getMessage());
                        keyFuture.completeExceptionally(t);
                        return null;
                    });

            List<String> keySets = keyFuture.get(30, TimeUnit.SECONDS);
            CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
            List<CompletableFuture<Void>> deleteFutures = new ArrayList<>();

            for (String keySet : keySets) {
                Set<byte[]> zsetKeys = redisTemplate.opsForSet().members(keySet);
                if (zsetKeys != null && !zsetKeys.isEmpty()) {
                    List<String> keysToDelete = zsetKeys.stream().map(String::new).collect(Collectors.toList());
                    keysToDelete.add(keySet);
                    deleteFutures.add(CompletableFuture.runAsync(() -> redisTemplate.delete(keysToDelete), cacheExecutor));
                }
            }

            CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
                    .thenAcceptAsync(v -> {
                        log.info("Cleared {} keys for groupId={}", keySets.size(), groupId);
                        deleteFuture.complete(null);
                    }, cacheExecutor)
                    .exceptionally(t -> {
                        meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "error_type", "clear").increment();
                        deleteFuture.completeExceptionally(t);
                        return null;
                    });

            deleteFuture.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            meterRegistry.counter("match_cache_errors", "groupId", groupId.toString(), "error_type", "clear").increment();
            log.error("Failed to clear matches for groupId={}: {}", groupId, e.getMessage());
            throw new CompletionException("Failed to clear matches", e);
        } finally {
            sample.stop(meterRegistry.timer("match_cache_clear_duration", "groupId", groupId.toString()));
        }
    }

    public void cacheMatchesFallback(List<GraphRecords.PotentialMatch> matches, UUID groupId, UUID domainId, String processingCycleId, Throwable t) {
        meterRegistry.counter("match_cache_fallbacks", "groupId", groupId.toString(), "domainId", domainId.toString(),
                "processingCycleId", processingCycleId, "error_type", "cache").increment(matches.size());
    }

    public AutoCloseableStream<Edge> streamEdgesFallback(UUID groupId, UUID domainId, String processingCycleId, int topK, Throwable t) {
        log.warn("Cache unavailable for groupId={}, domainId={}, processingCycleId={}, returning empty stream: {}", groupId, domainId, processingCycleId, t.getMessage());
        meterRegistry.counter("match_cache_fallbacks", "groupId", groupId.toString(), "domainId", domainId.toString(),
                "processingCycleId", processingCycleId, "error_type", "stream").increment();
        return new AutoCloseableStream<>(Stream.empty());
    }

    public Set<String> getCachedMatchKeysFallback(UUID groupId, UUID domainId, Throwable t) {
        log.warn("Cache unavailable for groupId={}, domainId={}: {}", groupId, domainId, t.getMessage());
        meterRegistry.counter("match_cache_fallbacks", "groupId", groupId.toString(), "domainId", domainId.toString(),
                "error_type", "keys").increment();
        return Collections.emptySet();
    }

    public void clearMatchesFallback(UUID groupId, Throwable t) {
        log.warn("Cache unavailable for groupId={}, skipping clear: {}", groupId, t.getMessage());
        meterRegistry.counter("match_cache_fallbacks", "groupId", groupId.toString(), "error_type", "clear").increment();
    }

    private byte[] serialize(GraphRecords.PotentialMatch match) {
        try {
            return objectMapper.writeValueAsBytes(match);
        } catch (JsonProcessingException e) {
            throw new CompletionException("Serialization failed for groupId=" + match.getGroupId(), e);
        }
    }

    private Edge toEdge(GraphRecords.PotentialMatch match) {
        return GraphRequestFactory.toEdge(match);
    }

    private List<String> scanKeys(String pattern) {
        List<String> keys = new ArrayList<>();
        ScanOptions scanOptions = ScanOptions.scanOptions().match(pattern).count(batchSize).build();
        try (Cursor<String> cursor = redisTemplate.scan(scanOptions)) {
            while (cursor.hasNext()) {
                keys.add(cursor.next());
            }
        } catch (Exception e) {
            log.warn("Failed to scan keys for pattern={}: {}", pattern, e.getMessage());
            throw new CompletionException("Key scan failed", e);
        }
        return keys;
    }
}