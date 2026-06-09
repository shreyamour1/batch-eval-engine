package com.example.batcheval.batch;

import com.example.batcheval.batch.model.PromptTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

// Unit test: no Kafka broker. We mock the KafkaTemplate and assert the publisher
// emits exactly one correctly-shaped message per prompt.
class PromptPublisherTest {

    private final KafkaTemplate<String, PromptTask> kafkaTemplate = mock();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PromptPublisher publisher =
            new PromptPublisher(kafkaTemplate, objectMapper, "prompt-jobs");

    @Test
    void publishesOneMessagePerPrompt() throws Exception {
        String json = """
                [
                  {"id": "p-1", "prompt": "hello"},
                  {"id": "p-2", "prompt": "world"},
                  {"id": "p-3", "prompt": "again"}
                ]
                """;
        var in = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        int count = publisher.publish("job-123", in);

        assertThat(count).isEqualTo(3);

        ArgumentCaptor<PromptTask> captor = ArgumentCaptor.forClass(PromptTask.class);
        verify(kafkaTemplate, times(3)).send(eq("prompt-jobs"), anyString(), captor.capture());

        assertThat(captor.getAllValues())
                .allMatch(t -> t.jobId().equals("job-123"));
        assertThat(captor.getAllValues())
                .extracting(PromptTask::promptId)
                .containsExactly("p-1", "p-2", "p-3");
    }

    @Test
    void emptyArrayPublishesNothing() throws Exception {
        var in = new ByteArrayInputStream("[]".getBytes(StandardCharsets.UTF_8));

        int count = publisher.publish("job-empty", in);

        assertThat(count).isZero();
        verify(kafkaTemplate, times(0)).send(anyString(), anyString(), any());
    }
}
