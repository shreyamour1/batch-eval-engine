package com.example.batcheval.worker;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.web.client.RestClient;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InferenceClientTest {

    @RegisterExtension
    static WireMockExtension wireMock = WireMockExtension.newInstance()
            .options(wireMockConfig().dynamicPort())
            .build();

    private InferenceClient client;

    @BeforeEach
    void setUp() {
        Retry retry = Retry.of("inference-test", RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(attempt -> 10L)
                .retryOnException(e -> e instanceof InferenceException ie && ie.isRetryable())
                .build());
        client = new InferenceClient(
                RestClient.builder(),
                retry,
                wireMock.baseUrl() + "/v1/completions");
    }

    @Test
    void returnsCompletionOnSuccess() {
        stubFor(post(urlEqualTo("/v1/completions"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"completion\":\"hello world\"}")));

        assertThat(client.complete("test prompt")).isEqualTo("hello world");
    }

    @Test
    void retriesOn429ThenSucceeds() {
        stubFor(post(urlEqualTo("/v1/completions"))
                .inScenario("rate-limit")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(429))
                .willSetStateTo("recovered"));
        stubFor(post(urlEqualTo("/v1/completions"))
                .inScenario("rate-limit")
                .whenScenarioStateIs("recovered")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"completion\":\"ok\"}")));

        assertThat(client.complete("retry me")).isEqualTo("ok");
        wireMock.verify(2, postRequestedFor(urlEqualTo("/v1/completions")));
    }

    @Test
    void doesNotRetryOn400() {
        stubFor(post(urlEqualTo("/v1/completions"))
                .willReturn(aResponse().withStatus(400).withBody("bad request")));

        assertThatThrownBy(() -> client.complete("bad"))
                .isInstanceOf(InferenceException.class)
                .satisfies(e -> assertThat(((InferenceException) e).isRetryable()).isFalse());

        wireMock.verify(1, postRequestedFor(urlEqualTo("/v1/completions")));
    }

    @Test
    void sendsPromptInRequestBody() {
        stubFor(post(urlEqualTo("/v1/completions"))
                .withRequestBody(equalToJson("{\"prompt\":\"my prompt\"}"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"completion\":\"done\"}")));

        client.complete("my prompt");
    }
}
