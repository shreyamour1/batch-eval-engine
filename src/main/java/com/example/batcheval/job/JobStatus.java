package com.example.batcheval.job;

public enum JobStatus {
    PENDING,      // accepted, not yet started publishing
    PUBLISHING,   // streaming prompts onto Kafka
    RUNNING,      // consumers processing prompts
    COMPLETED,    // all prompts processed (success or isolated error)
    FAILED        // publishing failed
}
