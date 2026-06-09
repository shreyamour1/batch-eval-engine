package com.example.batcheval.config;

import com.example.batcheval.worker.InferenceException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Resilience4jConfig {

    @Bean
    public Retry inferenceRetry(
            @Value("${inference.retry.max-attempts}") int maxAttempts,
            @Value("${inference.retry.initial-interval-ms}") long initialIntervalMs,
            @Value("${inference.retry.multiplier}") double multiplier,
            @Value("${inference.retry.jitter-factor}") double jitterFactor) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(
                        initialIntervalMs, multiplier, jitterFactor))
                .retryOnException(e -> e instanceof InferenceException ie && ie.isRetryable())
                .build();
        return Retry.of("inference", config);
    }
}
