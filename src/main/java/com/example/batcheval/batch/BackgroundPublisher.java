package com.example.batcheval.batch;

import com.example.batcheval.job.JobRegistry;
import com.example.batcheval.job.JobState;
import com.example.batcheval.job.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

// Runs the publishing work on a background thread (separate bean so Spring's
// @Async proxy actually applies - self-invocation would not be async).
@Component
public class BackgroundPublisher {

    private static final Logger log = LoggerFactory.getLogger(BackgroundPublisher.class);

    private final JobRegistry jobRegistry;
    private final PromptPublisher promptPublisher;

    public BackgroundPublisher(JobRegistry jobRegistry, PromptPublisher promptPublisher) {
        this.jobRegistry = jobRegistry;
        this.promptPublisher = promptPublisher;
    }

    @Async("publishExecutor")
    public void run(String jobId, Path tempFile) {
        JobState state = jobRegistry.find(jobId).orElseThrow();
        state.setStatus(JobStatus.PUBLISHING);
        try (InputStream in = Files.newInputStream(tempFile)) {
            int published = promptPublisher.publish(jobId, in);
            state.addPublished(published);
            state.markRunning(published);
        } catch (Exception e) {
            log.error("Publishing failed for job {}", jobId, e);
            state.setStatus(JobStatus.FAILED);
            state.setError(e.getMessage());
        } finally {
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                log.warn("Could not delete temp file {}", tempFile, e);
            }
        }
    }
}
