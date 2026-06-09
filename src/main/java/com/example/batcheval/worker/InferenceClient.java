package com.example.batcheval.worker;

import io.github.resilience4j.retry.Retry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

// Calls the upstream inference endpoint. Every request is wrapped in a
// Resilience4j Retry (exponential backoff + jitter) for 429 and transient 5xx.
@Component
public class InferenceClient {

    private final RestClient restClient;
    private final Retry retry;
    private final String inferenceUrl;

    public InferenceClient(RestClient.Builder restClientBuilder,
                           Retry inferenceRetry,
                           @Value("${inference.url}") String inferenceUrl) {
        this.restClient = restClientBuilder.build();
        this.retry = inferenceRetry;
        this.inferenceUrl = inferenceUrl;
    }

    public String complete(String prompt) {
        return Retry.decorateSupplier(retry, () -> doRequest(prompt)).get();
    }

    private String doRequest(String prompt) {
        return restClient.post()
                .uri(inferenceUrl)
                .body(new InferenceRequest(prompt))
                .exchange((request, response) -> {
                    HttpStatusCode status = response.getStatusCode();
                    if (status.is2xxSuccessful()) {
                        InferenceResponse body = response.bodyTo(InferenceResponse.class);
                        if (body == null || body.completion() == null) {
                            throw new InferenceException(status, "Empty completion response", false);
                        }
                        return body.completion();
                    }
                    String errorBody = response.bodyTo(String.class);
                    String message = "Inference failed with status " + status.value()
                            + (errorBody != null ? ": " + errorBody : "");
                    boolean retryable = InferenceException.isRetryableStatus(status.value());
                    throw new InferenceException(status, message, retryable);
                });
    }
}
