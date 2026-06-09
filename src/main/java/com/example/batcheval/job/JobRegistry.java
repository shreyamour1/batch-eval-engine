package com.example.batcheval.job;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

// In-memory registry of jobs. In a later step this is backed by Redis so status
// survives restarts and is shared across instances.
@Component
public class JobRegistry {

    private final Map<String, JobState> jobs = new ConcurrentHashMap<>();

    public JobState create(String jobId) {
        JobState state = new JobState(jobId);
        jobs.put(jobId, state);
        return state;
    }

    public Optional<JobState> find(String jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }
}
