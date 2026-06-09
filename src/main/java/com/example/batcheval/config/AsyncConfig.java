package com.example.batcheval.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

// Bounded executor for the background work of reading the uploaded file and
// publishing it to Kafka, so POST /jobs can return immediately.
@Configuration
public class AsyncConfig {

    @Bean(name = "publishExecutor")
    public Executor publishExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(4);
        executor.setQueueCapacity(50);
        executor.setThreadNamePrefix("publish-");
        executor.initialize();
        return executor;
    }
}
