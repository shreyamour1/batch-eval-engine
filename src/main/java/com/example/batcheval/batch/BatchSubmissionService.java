package com.example.batcheval.batch;

import com.example.batcheval.job.JobRegistry;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

@Service
public class BatchSubmissionService {

    private final JobRegistry jobRegistry;
    private final BackgroundPublisher backgroundPublisher;

    public BatchSubmissionService(JobRegistry jobRegistry, BackgroundPublisher backgroundPublisher) {
        this.jobRegistry = jobRegistry;
        this.backgroundPublisher = backgroundPublisher;
    }

    // Runs on the request thread, so it must be fast: register the job, copy the
    // upload to a temp file, kick off background publishing, and return the id.
    public String submit(MultipartFile file) throws IOException {
        String jobId = UUID.randomUUID().toString();
        jobRegistry.create(jobId);

        // Copy now: the MultipartFile is gone once the request completes, and
        // publishing happens afterwards on a background thread.
        Path tempFile = Files.createTempFile("batch-" + jobId + "-", ".json");
        file.transferTo(tempFile);

        backgroundPublisher.run(jobId, tempFile);   // async, returns immediately
        return jobId;
    }
}
