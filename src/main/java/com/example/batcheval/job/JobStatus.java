package com.example.batcheval.job;

public enum JobStatus {
    PENDING,      // accepted, not yet started publishing
    PUBLISHING,   // streaming prompts onto Kafka
    SUBMITTED,    // all prompts published (processing by consumers comes later)
    FAILED        // publishing failed
}
