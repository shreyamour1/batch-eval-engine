package com.example.batcheval.api;

import com.example.batcheval.api.dto.JobDownloadResponse;
import com.example.batcheval.api.dto.JobStatusResponse;
import com.example.batcheval.job.JobQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/job")
public class JobQueryController {

    private final JobQueryService jobQueryService;

    public JobQueryController(JobQueryService jobQueryService) {
        this.jobQueryService = jobQueryService;
    }

    @GetMapping("/{id}/status")
    public ResponseEntity<JobStatusResponse> status(@PathVariable("id") String jobId) {
        return jobQueryService.getStatus(jobId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}/download")
    public ResponseEntity<?> download(@PathVariable("id") String jobId) {
        return jobQueryService.getDownload(jobId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> {
                    if (!jobQueryService.jobExists(jobId)) {
                        return ResponseEntity.notFound().build();
                    }
                    return ResponseEntity.status(HttpStatus.CONFLICT)
                            .body(Map.of("message", "Job is not complete yet"));
                });
    }
}
