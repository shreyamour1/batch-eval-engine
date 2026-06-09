package com.example.batcheval.config;

import com.example.batcheval.batch.model.PromptTask;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

// Explicit producer config so the KafkaTemplate is strongly typed to PromptTask.
// (Spring Boot would auto-configure an Object/Object template; defining our own
// makes the type obvious and injection unambiguous.)
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, PromptTask> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // Keep messages clean: don't embed Java type headers. The consumer
        // (added in a later step) is told the target type explicitly.
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, PromptTask> kafkaTemplate(
            ProducerFactory<String, PromptTask> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
