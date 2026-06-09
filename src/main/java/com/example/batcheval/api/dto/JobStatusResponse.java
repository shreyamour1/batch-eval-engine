package com.example.batcheval.api.dto;

import java.time.Instant;

public record JobStatusResponse(
        String jobId,
        String status,
        int total,
        int succeeded,
        int failed,
        Instant createdAt,
        String error
) {
}
