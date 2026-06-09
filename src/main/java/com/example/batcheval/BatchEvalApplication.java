package com.example.batcheval;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync   // enables @Async background publishing
public class BatchEvalApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchEvalApplication.class, args);
    }
}
