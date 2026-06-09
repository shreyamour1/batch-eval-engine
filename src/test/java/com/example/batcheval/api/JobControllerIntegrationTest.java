package com.example.batcheval.api;

import com.example.batcheval.batch.model.PromptTask;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Integration test: spins up a real Kafka in Docker via Testcontainers, POSTs a
// batch file through the real web layer, then consumes the topic to prove the
// prompts were actually published. Requires Docker to be running.
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class JobControllerIntegrationTest {

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        // Disable the app consumer so this test can read the topic directly.
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired
    MockMvc mockMvc;

    @Value("${batch.topic}")
    String topic;

    @Test
    void postJobsAcceptsFileAndPublishesPrompts() throws Exception {
        String json = """
                [
                  {"id":"p-1","prompt":"hello"},
                  {"id":"p-2","prompt":"world"}
                ]
                """;
        MockMultipartFile file = new MockMultipartFile(
                "file", "sample_batch.json", "application/json",
                json.getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(multipart("/jobs").file(file))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.jobId").isNotEmpty())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        List<PromptTask> received = new ArrayList<>();
        try (Consumer<String, PromptTask> consumer = testConsumer()) {
            consumer.subscribe(List.of(topic));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, PromptTask> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(r -> received.add(r.value()));
                assertThat(received).hasSize(2);
            });
        }

        assertThat(received).extracting(PromptTask::prompt)
                .containsExactlyInAnyOrder("hello", "world");
        assertThat(received).allMatch(t -> t.jobId() != null && !t.jobId().isBlank());
    }

    private Consumer<String, PromptTask> testConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<PromptTask> valueDeserializer = new JsonDeserializer<>(PromptTask.class);
        valueDeserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), valueDeserializer).createConsumer();
    }
}
