package com.example.batcheval.api;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class BatchPipelineIntegrationTest {

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static final WireMockServer wireMock;

    static {
        wireMock = new WireMockServer(wireMockConfig().dynamicPort());
        wireMock.start();
    }

    @AfterAll
    static void stopWireMock() {
        wireMock.stop();
    }

    @DynamicPropertySource
    static void overrideProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("inference.url", () -> wireMock.baseUrl() + "/v1/completions");
        registry.add("batch.consumer-concurrency", () -> "2");
        registry.add("inference.retry.max-attempts", () -> "3");
        registry.add("inference.retry.initial-interval-ms", () -> "50");
    }

    @Autowired
    MockMvc mockMvc;

    @BeforeEach
    void stubInference() {
        stubFor(post(urlEqualTo("/v1/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"completion\":\"mocked response\"}")));
    }

    @Test
    void fullPipelineProcessesBatchAndReturnsResults() throws Exception {
        String json = """
                [
                  {"id":"p-1","prompt":"hello"},
                  {"id":"p-2","prompt":"world"}
                ]
                """;
        MockMultipartFile file = new MockMultipartFile(
                "file", "batch.json", "application/json",
                json.getBytes(StandardCharsets.UTF_8));

        MvcResult submit = mockMvc.perform(multipart("/jobs").file(file))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.jobId").isNotEmpty())
                .andReturn();

        String jobId = com.jayway.jsonpath.JsonPath.read(
                submit.getResponse().getContentAsString(), "$.jobId");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/job/" + jobId + "/status"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.status").value("COMPLETED"))
                        .andExpect(jsonPath("$.total").value(2))
                        .andExpect(jsonPath("$.succeeded").value(2)));

        mockMvc.perform(get("/job/" + jobId + "/download"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results.length()").value(2))
                .andExpect(jsonPath("$.results[0].completion").value("mocked response"));
    }

    @Test
    void retriesRateLimitedPrompts() throws Exception {
        stubFor(post(urlEqualTo("/v1/completions"))
                .inScenario("429-recovery")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(429))
                .willSetStateTo("ok"));
        stubFor(post(urlEqualTo("/v1/completions"))
                .inScenario("429-recovery")
                .whenScenarioStateIs("ok")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"completion\":\"after retry\"}")));

        String json = "[{\"id\":\"p-1\",\"prompt\":\"retry test\"}]";
        MockMultipartFile file = new MockMultipartFile(
                "file", "batch.json", "application/json",
                json.getBytes(StandardCharsets.UTF_8));

        MvcResult submit = mockMvc.perform(multipart("/jobs").file(file))
                .andExpect(status().isAccepted())
                .andReturn();
        String jobId = com.jayway.jsonpath.JsonPath.read(
                submit.getResponse().getContentAsString(), "$.jobId");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/job/" + jobId + "/status"))
                        .andExpect(jsonPath("$.status").value("COMPLETED"))
                        .andExpect(jsonPath("$.succeeded").value(1)));

        String body = mockMvc.perform(get("/job/" + jobId + "/download"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(body).contains("after retry");
    }
}
