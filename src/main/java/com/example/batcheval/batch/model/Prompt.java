package com.example.batcheval.batch.model;

// One item from the uploaded sample_batch.json array, e.g.
//   {"id": "p-00001", "prompt": "Summarize ..."}
public record Prompt(String id, String prompt) {
}
