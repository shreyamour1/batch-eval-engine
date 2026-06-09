package com.example.batcheval.batch;

import com.example.batcheval.batch.model.Prompt;
import com.example.batcheval.batch.model.PromptTask;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.UUID;

// Reads prompts out of the input stream ONE AT A TIME and publishes each to
// Kafka. Streaming the parse is what keeps memory flat regardless of file size:
// the whole array is never held in memory at once.
@Component
public class PromptPublisher {

    private static final Logger log = LoggerFactory.getLogger(PromptPublisher.class);

    private final KafkaTemplate<String, PromptTask> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public PromptPublisher(KafkaTemplate<String, PromptTask> kafkaTemplate,
                           ObjectMapper objectMapper,
                           @Value("${batch.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    // Returns the number of prompts published.
    public int publish(String jobId, InputStream inputStream) throws Exception {
        int count = 0;
        try (MappingIterator<Prompt> it =
                     objectMapper.readerFor(Prompt.class).readValues(inputStream)) {
            while (it.hasNext()) {
                Prompt prompt = it.next();
                String promptId = (prompt.id() != null && !prompt.id().isBlank())
                        ? prompt.id()
                        : UUID.randomUUID().toString();
                PromptTask task = new PromptTask(jobId, promptId, prompt.prompt());
                // Key by promptId so prompts spread evenly across partitions.
                kafkaTemplate.send(topic, promptId, task);
                count++;
            }
        }
        log.info("Published {} prompts for job {}", count, jobId);
        return count;
    }
}
