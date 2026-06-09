package com.example.batcheval.job;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

// Mutable per-job state. Held in memory for now; moves to Redis in a later step.
public class JobState {

    private final String jobId;
    private final Instant createdAt = Instant.now();
    private final AtomicInteger publishedCount = new AtomicInteger(0);
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

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
}
