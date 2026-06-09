package com.example.batcheval.api;

import com.example.batcheval.job.JobRegistry;
import com.example.batcheval.job.JobState;
import com.example.batcheval.job.JobStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(JobQueryController.class)
@Import({com.example.batcheval.job.JobQueryService.class, JobRegistry.class})
class JobQueryControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    JobRegistry jobRegistry;

    @Test
    void statusReturns404ForUnknownJob() throws Exception {
        mockMvc.perform(get("/job/unknown/status"))
                .andExpect(status().isNotFound());
    }

    @Test
    void statusReturnsProgress() throws Exception {
        JobState state = jobRegistry.create("job-1");
        state.markRunning(3);
        state.recordSuccess("p-1", "ok");

        mockMvc.perform(get("/job/job-1/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value("job-1"))
                .andExpect(jsonPath("$.status").value("RUNNING"))
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.succeeded").value(1))
                .andExpect(jsonPath("$.failed").value(0));
    }

    @Test
    void downloadReturns409UntilComplete() throws Exception {
        JobState state = jobRegistry.create("job-2");
        state.markRunning(1);

        mockMvc.perform(get("/job/job-2/download"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("Job is not complete yet"));
    }

    @Test
    void downloadReturnsResultsWhenComplete() throws Exception {
        JobState state = jobRegistry.create("job-3");
        state.markRunning(1);
        state.recordSuccess("p-1", "answer");

        mockMvc.perform(get("/job/job-3/download"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").value("job-3"))
                .andExpect(jsonPath("$.results[0].promptId").value("p-1"))
                .andExpect(jsonPath("$.results[0].status").value("SUCCESS"))
                .andExpect(jsonPath("$.results[0].completion").value("answer"));
    }
}
