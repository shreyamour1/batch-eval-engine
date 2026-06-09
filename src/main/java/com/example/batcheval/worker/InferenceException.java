package com.example.batcheval.worker;

import org.springframework.http.HttpStatusCode;

public class InferenceException extends RuntimeException {

    private final HttpStatusCode statusCode;
    private final boolean retryable;

    public InferenceException(HttpStatusCode statusCode, String message, boolean retryable) {
        super(message);
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    public HttpStatusCode getStatusCode() {
        return statusCode;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public static boolean isRetryableStatus(int status) {
        return status == 429 || status == 502 || status == 503 || status == 504 || status == 500;
    }
}
