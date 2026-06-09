package com.example.batcheval.worker;

import com.example.batcheval.batch.model.PromptTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

// Consumer group worker: bounded by spring.kafka.listener.concurrency.
// Each message is processed independently; failures are isolated per prompt.
@Component
public class PromptWorker {

    private static final Logger log = LoggerFactory.getLogger(PromptWorker.class);

    private final ResultWriter resultWriter;

    public PromptWorker(ResultWriter resultWriter) {
        this.resultWriter = resultWriter;
    }

    @KafkaListener(
            topics = "${batch.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${batch.consumer-concurrency}"
    )
    public void consume(PromptTask task, Acknowledgment ack) {
        try {
            resultWriter.process(task);
        } catch (Exception e) {
            log.error("Unexpected error processing prompt {} for job {}",
                    task.promptId(), task.jobId(), e);
        } finally {
            ack.acknowledge();
        }
    }
}
