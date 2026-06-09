package com.example.batcheval.job;

// One outcome for a single prompt. Stored idempotently by promptId.
public record PromptResult(
        String promptId,
        String status,
        String completion,
        String error
) {
    public static PromptResult success(String promptId, String completion) {
        return new PromptResult(promptId, "SUCCESS", completion, null);
    }

    public static PromptResult failure(String promptId, String error) {
        return new PromptResult(promptId, "ERROR", null, error);
    }
}
