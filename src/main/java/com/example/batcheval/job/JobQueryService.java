package com.example.batcheval.job;

import com.example.batcheval.api.dto.JobDownloadResponse;
import com.example.batcheval.api.dto.JobStatusResponse;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class JobQueryService {

    private final JobRegistry jobRegistry;

    public JobQueryService(JobRegistry jobRegistry) {
        this.jobRegistry = jobRegistry;
    }

    public Optional<JobStatusResponse> getStatus(String jobId) {
        return jobRegistry.find(jobId).map(this::toStatusResponse);
    }

    public Optional<JobDownloadResponse> getDownload(String jobId) {
        return jobRegistry.find(jobId)
                .filter(state -> state.getStatus() == JobStatus.COMPLETED)
                .map(state -> new JobDownloadResponse(jobId, state.getResults()));
    }

    public boolean jobExists(String jobId) {
        return jobRegistry.find(jobId).isPresent();
    }

    private JobStatusResponse toStatusResponse(JobState state) {
        return new JobStatusResponse(
                state.getJobId(),
                state.getStatus().name(),
                state.getTotal(),
                state.getSucceeded(),
                state.getFailed(),
                state.getCreatedAt(),
                state.getError()
        );
    }
}
