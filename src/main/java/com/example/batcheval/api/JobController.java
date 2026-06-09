package com.example.batcheval.api;

import com.example.batcheval.api.dto.JobSubmissionResponse;
import com.example.batcheval.batch.BatchSubmissionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final BatchSubmissionService submissionService;

    public JobController(BatchSubmissionService submissionService) {
        this.submissionService = submissionService;
    }

    // POST /jobs   (multipart form field name: "file")
    // Returns 202 Accepted with a Job ID immediately; publishing runs in the background.
    @PostMapping
    public ResponseEntity<JobSubmissionResponse> submit(@RequestParam("file") MultipartFile file)
            throws IOException {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body(new JobSubmissionResponse(null, "EMPTY_FILE"));
        }
        String jobId = submissionService.submit(file);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new JobSubmissionResponse(jobId, "ACCEPTED"));
    }
}
