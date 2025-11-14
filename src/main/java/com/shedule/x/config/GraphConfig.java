package com.shedule.x.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.shedule.x.builder.FlatEdgeBuildingStrategy;
import com.shedule.x.builder.MetadataEdgeBuildingStrategy;
import com.shedule.x.processors.*;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collections;
import java.util.concurrent.*;

@Slf4j
@Configuration
public class GraphConfig {

    @Bean("lshExecutor")
    public ExecutorService lshExecutorService(MeterRegistry meterRegistry) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("lsh-executor-%d").build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                4, 8, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(500),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        meterRegistry.counter("lsh_executor_rejections").increment();
                        log.warn("LSH task rejected: queue size={}", e.getQueue().size());
                        super.rejectedExecution(r, e);
                    }
                }
        );
        executor.allowCoreThreadTimeOut(false);
        return executor;
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
    @Scope("prototype")
    public LSHIndexImpl lshIndex(
            @Value("${lsh.num-hash-tables:16}") int numHashTables,
            @Value("${lsh.num-bands:16}") int numBands,
            @Value("${graph.builder.candidate.limit:1000}") int topK,
            MeterRegistry meterRegistry,
            @Qualifier("lshExecutor") ExecutorService lshExecutor,
            GraphStore graphStore) {
        return new LSHIndexImpl(
                LSHConfig.builder().numBands(numBands).numHashTables(numHashTables).topK(topK).build(),
                meterRegistry,
                lshExecutor,
                graphStore
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
            @Qualifier("graphBuildExecutor") ExecutorService executor,
            @Value("${graph.builder.candidate.limit:200}") Integer candidateLimit,
            @Value("${graph.builder.similarity.threshold:0.01}") Double similarityThreshold) {
        return new MetadataEdgeBuildingStrategy(
                EdgeBuildingConfig.builder()
                        .candidateLimit(candidateLimit).similarityThreshold(similarityThreshold)
                        .chunkTimeoutSeconds(60).maxRetries(3).retryDelayMillis(10000)
                        .build(),
                lshIndexImpl,
                encoder,
                executor,
                edgeProcessor
        );
    }

    @Bean
    public Integer candidateLimit(@Value("${graph.builder.candidate.limit:1000}") Integer limit) {
        return limit;
    }

    @Bean
    public Double similarityThreshold(@Value("${graph.builder.similarity.threshold:0.01}") Double threshold) {
        return threshold;
    }
}