package com.example.batcheval.api.dto;

import com.example.batcheval.job.PromptResult;

import java.util.List;

public record JobDownloadResponse(String jobId, List<PromptResult> results) {
}
