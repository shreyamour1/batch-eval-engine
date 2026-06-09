package com.example.batcheval.worker;

import com.example.batcheval.batch.model.PromptTask;
import com.example.batcheval.job.JobRegistry;
import com.example.batcheval.job.JobState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

// The "gather" side: calls inference, records each outcome idempotently by
// promptId, and updates per-job counters.
@Component
public class ResultWriter {

    private static final Logger log = LoggerFactory.getLogger(ResultWriter.class);

    private final JobRegistry jobRegistry;
    private final InferenceClient inferenceClient;

    public ResultWriter(JobRegistry jobRegistry, InferenceClient inferenceClient) {
        this.jobRegistry = jobRegistry;
        this.inferenceClient = inferenceClient;
    }

    public void process(PromptTask task) {
        JobState state = jobRegistry.find(task.jobId()).orElse(null);
        if (state == null) {
            log.warn("Ignoring prompt {} for unknown job {}", task.promptId(), task.jobId());
            return;
        }

        try {
            String completion = inferenceClient.complete(task.prompt());
            state.recordSuccess(task.promptId(), completion);
        } catch (InferenceException e) {
            if (e.isRetryable()) {
                // Retries exhausted; record as a permanent failure for this prompt.
                state.recordFailure(task.promptId(), e.getMessage());
            } else {
                state.recordFailure(task.promptId(), e.getMessage());
            }
        } catch (Exception e) {
            state.recordFailure(task.promptId(), e.getMessage());
        }
    }
}
