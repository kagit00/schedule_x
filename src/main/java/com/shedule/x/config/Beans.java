package com.shedule.x.config;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.shedule.x.config.factory.QueueManagerFactory;
import com.shedule.x.dto.NodeResponse;
import com.shedule.x.config.factory.NodeResponseFactory;
import com.shedule.x.config.factory.ResponseFactory;
import com.shedule.x.processors.QueueManagerImpl;
import io.github.resilience4j.common.bulkhead.configuration.ThreadPoolBulkheadConfigCustomizer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;


@Configuration
@Slf4j
public class Beans {

    @Bean
    public ResponseFactory<NodeResponse> nodeResponseFactory() {
        return new NodeResponseFactory();
    }

    @Bean(name = "nodesFetchExecutor")
    public ThreadPoolTaskExecutor nodesFetchExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(3);
        executor.setMaxPoolSize(6);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("nodes-fetch-");
        executor.initialize();
        return executor;
    }


    @Bean
    public Executor matchTransferGroupExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("MatchTransferGroup-");
        executor.setRejectedExecutionHandler((r, e) -> log.warn("Task rejected for match transfer, consider increasing pool size."));
        executor.initialize();
        return executor;
    }

    @Bean
    public Executor matchTransferExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(1000);
        executor.setThreadNamePrefix("potential-match-transfer-");
        executor.setRejectedExecutionHandler((r, e) -> log.warn("Task rejected for match transfer, consider increasing pool size"));
        executor.initialize();
        return executor;
    }

    @Bean("persistenceExecutor")
    public ExecutorService persistenceExecutor(MeterRegistry meterRegistry) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("persistence-executor-%d")
                .build();

        return new ThreadPoolExecutor(
                10, 20,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(Integer.MAX_VALUE),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        meterRegistry.counter("persistence_executor_rejections").increment();
                        log.warn("persistence task rejected: queue size={}", e.getQueue().size());
                        super.rejectedExecution(r, e);
                    }
                }
        );
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService cpuExecutor() {
        return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), r -> new Thread(r, "cpu-executor-thread"));
    }

    @Bean("ioExecutorService")
    public ExecutorService ioExecutorService() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("io-executor-%d").build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                4, 8, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(100),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        super.rejectedExecution(r, e);
                    }
                }
        );
        executor.allowCoreThreadTimeOut(false);
        return executor;
    }

    @Bean(name = "cacheExecutor")
    public ExecutorService cacheExecutor() {
        return Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "cache-executor");
            t.setDaemon(true);
            return t;
        });
    }

    @Bean(name = "queueFlushExecutor")
    public ExecutorService queueFlushExecutor() {
        return Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "queue-flush-executor");
            t.setDaemon(true);
            return t;
        });
    }

    @Bean("matchCreationExecutorService")
    public ExecutorService matchCreationExecutorService(MeterRegistry meterRegistry) {
        int cpus = Runtime.getRuntime().availableProcessors();

        int corePoolSize = Math.max(4, cpus);
        int maxPoolSize = cpus * 4;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("match-create-%d")
                .setThreadFactory(Executors.defaultThreadFactory())
                .setUncaughtExceptionHandler((t, e) ->
                        System.err.printf("Thread %s threw exception: %s%n", t.getName(), e.getMessage()))
                .build();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy()
        );

        executor.allowCoreThreadTimeOut(true);
        executor.prestartAllCoreThreads();

        return executor;
    }

    @Bean("semaphoreExecutor")
    public ExecutorService semaphoreExecutor() {
        return Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("semaphore-%d").build()
        );
    }


    @Bean("graphBuildExecutor")
    public ExecutorService graphBuildExecutor(MeterRegistry meterRegistry) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("graph-build-%d").build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                8, 16, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        meterRegistry.counter("match_creation_executor_rejections").increment();
                        log.warn("Match creation task rejected: queue size={}", e.getQueue().size());
                        super.rejectedExecution(r, e);
                    }
                }
        );
        executor.allowCoreThreadTimeOut(false);
        return executor;
    }

    @Bean(name = "watchdogExecutor")
    public ScheduledExecutorService watchdogExecutor() {
        int corePoolSize = 2;
        return Executors.newScheduledThreadPool(corePoolSize);
    }


    @Bean("graphExecutorService")
    public ExecutorService graphExecutorService() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("graph-ex-%d").build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                8, 16, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        super.rejectedExecution(r, e);
                    }
                }
        );
        executor.allowCoreThreadTimeOut(false);
        return executor;
    }

    @Bean(name = "generalTaskExecutor")
    public ThreadPoolTaskExecutor generalTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("general-");
        executor.initialize();
        return executor;
    }

    @Bean(name = "matchesTransferExecutor")
    public TaskExecutor matchesTransferExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("MatchesTransfer-");
        executor.initialize();
        return executor;
    }

    @Bean
    public RetryTemplate retryTemplate() {
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3, Map.of(
                SQLException.class, true,
                DataAccessException.class, true
        ));
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);
        backOffPolicy.setMultiplier(2.0);
        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(retryPolicy);
        template.setBackOffPolicy(backOffPolicy);
        return template;
    }


    @Bean(name = "blossomExecutor")
    public TaskExecutor blossomExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(25);
        executor.setThreadNamePrefix("Blossom-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    @Bean(name = "nodesImportExecutor")
    public ThreadPoolTaskExecutor nodesImportExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("NodesImport-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }

    @Bean(name = "matchesStorageExecutor")
    public ExecutorService matchesStorageExecutor() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("match-storage-executor-%d").build();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                8, 16, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(200),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        super.rejectedExecution(r, e);
                    }
                }
        );
        executor.allowCoreThreadTimeOut(false);
        return executor;
    }


    @Bean
    @Qualifier("queueFlushScheduler")
    public ScheduledExecutorService queueFlushScheduler() {
        return Executors.newScheduledThreadPool(8, r -> {
            Thread t = new Thread(r, "queue-flush-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    @Bean(name = "matchesProcessExecutor")
    public ExecutorService matchesProcessExecutor(@Value("${match.semaphore.permits:8}") int permits) {
        return new ThreadPoolExecutor(
                permits,
                permits,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(100),
                new ThreadFactoryBuilder().setNameFormat("match-pro-%d").build()
        );
    }

}
