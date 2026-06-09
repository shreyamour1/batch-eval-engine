package com.example.batcheval.batch.model;

// The message we publish to Kafka for each prompt. It carries the jobId so the
// consumer (added later) knows which job the prompt belongs to.
public record PromptTask(String jobId, String promptId, String prompt) {
}
