package com.shedule.x.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Semaphore;

@Configuration
@RequiredArgsConstructor
public class ExportServiceConfiguration {

    private final ExportConfig exportConfig;

    @Bean
    public Semaphore exportSemaphore() {
        return new Semaphore(exportConfig.getSemaphoreLimit());
    }
}