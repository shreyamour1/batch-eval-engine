package com.example.batcheval.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// Creates the topic on startup. Multiple partitions so the consumer group
// (added later) can scale out across them.
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic promptJobsTopic(@Value("${batch.topic}") String topic) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
