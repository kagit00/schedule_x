package com.shedule.x.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.shedule.x.builder.FlatEdgeBuildingStrategy;
import com.shedule.x.builder.MetadataEdgeBuildingStrategy;
import com.shedule.x.config.factory.NodePriorityProvider;
import com.shedule.x.processors.*;
import com.shedule.x.service.NodeDataService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Configuration
public class GraphConfig {

    @Bean
    public NodePriorityProvider nodePriorityProvider(@Lazy LSHIndex lshIndex) {
        return lshIndex::getNodePriorityScore;
    }

    @Bean("lshExecutor")
    public ExecutorService lshExecutorService(MeterRegistry meterRegistry) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("lsh-executor-%d").build();

        return new ThreadPoolExecutor(
                6, 12,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(Integer.MAX_VALUE),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Bean
    public ThreadLocal<Map<Integer, List<UUID>>> lshBatchBuffer() {
        return ThreadLocal.withInitial(HashMap::new);
    }


    @Bean(name = "matchChunkExecutor")
    public ThreadPoolTaskExecutor matchChunkExecutor() {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(4);
        exec.setMaxPoolSize(8);
        exec.setQueueCapacity(100);
        exec.setThreadNamePrefix("Match-Chunk-");
        exec.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        exec.initialize();
        return exec;
    }

    @Bean
    public LSHIndexImpl lshIndex(
            @Value("${lsh.num-hash-tables:120}") int numHashTables,
            @Value("${lsh.num-bands:1}") int numBands,
            @Value("${graph.builder.candidate.limit:10000}") int topK,
            MeterRegistry meterRegistry,
            @Qualifier("lshExecutor") ExecutorService lshExecutor,
            GraphStore graphStore,
            ThreadLocal<Map<Integer, List<UUID>>> lshBatchBuffer) {
        return new LSHIndexImpl(
                LSHConfig.builder().numBands(numBands).numHashTables(numHashTables).topK(topK).build(),
                meterRegistry,
                lshExecutor,
                graphStore,
                lshBatchBuffer
        );
    }


    @Bean
    public FlatEdgeBuildingStrategy flatEdgeBuildingStrategy(
            PotentialMatchSaver saver,
            @Qualifier("matchCreationExecutorService") ExecutorService executor) {
        return new FlatEdgeBuildingStrategy(Collections.emptyList(), saver, executor);
    }

    @Bean
    public MetadataEdgeBuildingStrategy metadataEdgeBuildingStrategy(
            EdgeProcessor edgeProcessor,
            LSHIndexImpl lshIndexImpl,
            MetadataEncoder encoder,
            NodeDataService nodeDataService,
            @Qualifier("graphBuildExecutor") ExecutorService executor,
            @Value("${graph.builder.candidate.limit:10000}") Integer candidateLimit,
            @Value("${graph.builder.similarity.threshold:0.01}") Double similarityThreshold) {
        return new MetadataEdgeBuildingStrategy(
                EdgeBuildingConfig.builder()
                        .candidateLimit(candidateLimit).similarityThreshold(similarityThreshold)
                        .chunkTimeoutSeconds(60).maxRetries(3).retryDelayMillis(10000)
                        .build(),
                lshIndexImpl,
                encoder,
                executor,
                edgeProcessor,
                nodeDataService
        );
    }

    @Bean
    public Integer candidateLimit(@Value("${graph.builder.candidate.limit:10000}") Integer limit) {
        return limit;
    }

    @Bean
    public Double similarityThreshold(@Value("${graph.builder.similarity.threshold:0.01}") Double threshold) {
        return threshold;
    }
}