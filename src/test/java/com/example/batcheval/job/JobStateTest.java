package com.example.batcheval.job;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JobStateTest {

    @Test
    void marksCompletedWhenAllPromptsProcessed() {
        JobState state = new JobState("job-1");
        state.markRunning(2);

        state.recordSuccess("p-1", "a");
        assertThat(state.getStatus()).isEqualTo(JobStatus.RUNNING);

        state.recordFailure("p-2", "boom");
        assertThat(state.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(state.getSucceeded()).isEqualTo(1);
        assertThat(state.getFailed()).isEqualTo(1);
    }

    @Test
    void duplicateSuccessDoesNotDoubleCount() {
        JobState state = new JobState("job-1");
        state.markRunning(1);

        state.recordSuccess("p-1", "first");
        state.recordSuccess("p-1", "second");

        assertThat(state.getSucceeded()).isEqualTo(1);
        assertThat(state.getResults().getFirst().completion()).isEqualTo("second");
    }

    @Test
    void emptyBatchCompletesImmediately() {
        JobState state = new JobState("job-empty");
        state.markRunning(0);

        assertThat(state.getStatus()).isEqualTo(JobStatus.COMPLETED);
    }
}
