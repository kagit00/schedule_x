package com.shedule.x.config;

import com.shedule.x.dto.NodeResponse;
import com.shedule.x.config.factory.NodeResponseFactory;
import com.shedule.x.config.factory.ResponseFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.ThreadPoolExecutor;


@Configuration
public class Beans {

    @Bean
    public ResponseFactory<NodeResponse> nodeResponseFactory() {
        return new NodeResponseFactory();
    }

    @Bean(name = "matchesCreationExecutor")
    public TaskExecutor matchingExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("MatchesCreation-");
        executor.initialize();
        return executor;
    }

    @Bean(name = "matchesTransferExecutor")
    public TaskExecutor matchesTransferExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(20);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("MatchesTransfer-");
        executor.initialize();
        return executor;
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
}
