package com.example.batcheval.job;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// Mutable per-job state. Held in memory for now; moves to Redis in a later step.
public class JobState {

    private final String jobId;
    private final Instant createdAt = Instant.now();
    private final AtomicInteger publishedCount = new AtomicInteger(0);
    private final AtomicInteger total = new AtomicInteger(0);
    private final AtomicInteger succeeded = new AtomicInteger(0);
    private final AtomicInteger failed = new AtomicInteger(0);
    private final ConcurrentHashMap<String, PromptResult> results = new ConcurrentHashMap<>();
    private volatile JobStatus status;
    private volatile String error;

    public JobState(String jobId) {
        this.jobId = jobId;
        this.status = JobStatus.PENDING;
    }

    public String getJobId() { return jobId; }
    public Instant getCreatedAt() { return createdAt; }

    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }

    public int getPublishedCount() { return publishedCount.get(); }
    public void addPublished(int n) { publishedCount.addAndGet(n); }

    public int getTotal() { return total.get(); }

    public int getSucceeded() { return succeeded.get(); }
    public int getFailed() { return failed.get(); }

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }

    public void recordSuccess(String promptId, String completion) {
        PromptResult existing = results.get(promptId);
        if (existing != null && "SUCCESS".equals(existing.status())) {
            return;
        }
        if (existing != null && "ERROR".equals(existing.status())) {
            failed.decrementAndGet();
        }
        results.put(promptId, PromptResult.success(promptId, completion));
        succeeded.incrementAndGet();
        tryMarkCompleted();
    }

    public void recordFailure(String promptId, String errorMessage) {
        if (results.containsKey(promptId)) {
            return;
        }
        results.put(promptId, PromptResult.failure(promptId, errorMessage));
        failed.incrementAndGet();
        tryMarkCompleted();
    }

    public List<PromptResult> getResults() {
        return results.values().stream()
                .sorted(Comparator.comparing(PromptResult::promptId))
                .toList();
    }

    public void markRunning(int promptTotal) {
        total.set(promptTotal);
        if (promptTotal == 0) {
            status = JobStatus.COMPLETED;
        } else {
            status = JobStatus.RUNNING;
        }
    }

    private void tryMarkCompleted() {
        if (status == JobStatus.RUNNING && succeeded.get() + failed.get() >= total.get()) {
            status = JobStatus.COMPLETED;
        }
    }
}
