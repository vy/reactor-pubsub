/*
 * Copyright 2019-2020 Volkan Yazıcı
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permits and
 * limitations under the License.
 */

package com.vlkan.pubsub;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.vlkan.pubsub.jackson.JacksonHelpers;
import com.vlkan.pubsub.model.PubsubAckRequest;
import com.vlkan.pubsub.model.PubsubDraftedMessage;
import com.vlkan.pubsub.model.PubsubPublishRequest;
import com.vlkan.pubsub.model.PubsubPublishResponse;
import com.vlkan.pubsub.model.PubsubPullRequest;
import com.vlkan.pubsub.model.PubsubPullResponse;
import com.vlkan.pubsub.model.PubsubPullResponseFixture;
import com.vlkan.pubsub.model.PubsubReceivedMessage;
import io.micrometer.core.instrument.Clock;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.prometheus.client.CollectorRegistry;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class PubsubClientTest {

    @Rule
    public final WireMockRule serverMockRule =
            new WireMockRule(WireMockConfiguration.options().port(8888), true);

    private static final String PROJECT_NAME = "test-project";

    private static final String SUBSCRIPTION_NAME = "test-subscription";

    private static final String TOPIC_NAME = "test-topic";

    private static final String PULL_REQUEST_RELATIVE_PATH =
            PubsubClient.createPullRequestRelativePath(PROJECT_NAME, SUBSCRIPTION_NAME);

    private static final String ACK_REQUEST_RELATIVE_PATH =
            PubsubClient.createAckRequestRelativePath(PROJECT_NAME, SUBSCRIPTION_NAME);

    private static final String PUBLISH_REQUEST_RELATIVE_PATH =
            PubsubClient.createPublishRequestRelativePath(PROJECT_NAME, TOPIC_NAME);

    private static final PubsubPullRequest PULL_REQUEST =
            new PubsubPullRequest(true, Integer.MAX_VALUE);

    private static final PubsubPullResponse PULL_RESPONSE =
            PubsubPullResponseFixture.createRandomPullResponse(10);

    private static final PubsubAckRequest ACK_REQUEST =
            new PubsubAckRequest(PULL_RESPONSE
                    .getReceivedMessages()
                    .stream()
                    .map(PubsubReceivedMessage::getAckId)
                    .collect(Collectors.toList()));

    private static final PubsubPublishRequest PUBLISH_REQUEST =
            new PubsubPublishRequest(PULL_RESPONSE
                    .getReceivedMessages()
                    .stream()
                    .map(receivedMessage -> {
                        byte[] payload = receivedMessage.getPayload();
                        return new PubsubDraftedMessage(payload);
                    })
                    .collect(Collectors.toList()));

    private static final PubsubPublishResponse PUBLISH_RESPONSE =
            new PubsubPublishResponse(PULL_RESPONSE
                    .getReceivedMessages()
                    .stream()
                    .map(PubsubReceivedMessage::getId)
                    .collect(Collectors.toList()));

    private static final PrometheusMeterRegistry PROMETHEUS_METER_REGISTRY =
            new PrometheusMeterRegistry(
                    PrometheusConfig.DEFAULT,
                    new CollectorRegistry(),
                    Clock.SYSTEM);

    private static final String PROMETHEUS_TAG_DISCREPANCY_ERROR_MESSAGE =
            "Prometheus requires that all meters with the same name have the same set of tag keys.";

    @Test
    public void test_prometheus_error_on_tag_discrepancy_1() {
        String meterName = "test_prometheus_error_on_tag_discrepancy_1";
        PROMETHEUS_METER_REGISTRY.counter(meterName, "k1", "v1");
        Assertions
                .assertThatThrownBy(() -> PROMETHEUS_METER_REGISTRY.counter(meterName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PROMETHEUS_TAG_DISCREPANCY_ERROR_MESSAGE);
    }

    @Test
    public void test_prometheus_error_on_tag_discrepancy_2() {
        String meterName = "test_prometheus_error_on_tag_discrepancy_2";
        PROMETHEUS_METER_REGISTRY.counter(meterName);
        Assertions
                .assertThatThrownBy(() -> PROMETHEUS_METER_REGISTRY.counter(meterName, "k1", "v1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PROMETHEUS_TAG_DISCREPANCY_ERROR_MESSAGE);
    }

    @Test
    public void test_prometheus_error_on_tag_discrepancy_3() {
        String meterName = "test_prometheus_error_on_tag_discrepancy_3";
        PROMETHEUS_METER_REGISTRY.counter(meterName, "k1", "v1");
        Assertions
                .assertThatThrownBy(() -> PROMETHEUS_METER_REGISTRY.counter(meterName, "k2", "v2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(PROMETHEUS_TAG_DISCREPANCY_ERROR_MESSAGE);
    }

    @Test
    public void test_metrics_against_prometheus() {

        // Stub pull response.
        String pullResponseJson = JacksonHelpers.writeValueAsString(PULL_RESPONSE);
        serverMockRule.addStubMapping(
                WireMock.stubFor(WireMock
                        .post(WireMock.urlEqualTo(PULL_REQUEST_RELATIVE_PATH))
                        .willReturn(WireMock
                                .aResponse()
                                .withHeader(
                                        HttpHeaderNames.CONTENT_TYPE.toString(),
                                        HttpHeaderValues.APPLICATION_JSON.toString())
                                .withBody(pullResponseJson))));

        // Stub ack response.
        serverMockRule.addStubMapping(
                WireMock.stubFor(WireMock
                        .post(WireMock.urlEqualTo(ACK_REQUEST_RELATIVE_PATH))
                        .willReturn(WireMock
                                .aResponse()
                                .withHeader(
                                        HttpHeaderNames.CONTENT_TYPE.toString(),
                                        HttpHeaderValues.APPLICATION_JSON.toString()))));

        // Stub publish response.
        String publishResponseJson = JacksonHelpers.writeValueAsString(PUBLISH_RESPONSE);
        serverMockRule.addStubMapping(
                WireMock.stubFor(WireMock
                        .post(WireMock.urlEqualTo(PUBLISH_REQUEST_RELATIVE_PATH))
                        .willReturn(WireMock
                                .aResponse()
                                .withHeader(
                                        HttpHeaderNames.CONTENT_TYPE.toString(),
                                        HttpHeaderValues.APPLICATION_JSON.toString())
                                .withBody(publishResponseJson))));

        // Create Pub/Sub client using Prometheus meter registry.
        PubsubClientConfig clientConfig = PubsubClientConfig
                .builder()
                .setBaseUrl(serverMockRule.baseUrl())
                .build();
        PubsubAccessTokenCache accessTokenCache = PubsubAccessTokenCacheFixture.getInstance();
        PubsubClient client = PubsubClient
                .builder()
                .setConfig(clientConfig)
                .setAccessTokenCache(accessTokenCache)
                .setMeterRegistry(PROMETHEUS_METER_REGISTRY)
                .build();

        // Execute random requests to move the statistics.
        for (int trialIndex = 0; trialIndex < 100; trialIndex++) {
            int operationIndex = trialIndex % 3;
            switch (operationIndex) {

                // Pull
                case 0: {
                    PubsubPullResponse retrievedPullResponse = client
                            .pull(PROJECT_NAME, SUBSCRIPTION_NAME, PULL_REQUEST)
                            .block(Duration.ofSeconds(3));
                    Assertions
                            .assertThat(retrievedPullResponse)
                            .as("trialIndex=%d", trialIndex)
                            .isEqualTo(PULL_RESPONSE);
                    break;
                }

                // Ack
                case 1:
                    client
                            .ack(PROJECT_NAME, SUBSCRIPTION_NAME, ACK_REQUEST)
                            .block(Duration.ofSeconds(3));
                    break;

                // Publish
                case 2: {
                    PubsubPublishResponse retrievedPublishResponse = client
                            .publish(PROJECT_NAME, TOPIC_NAME, PUBLISH_REQUEST)
                            .block(Duration.ofSeconds(3));
                    Assertions
                            .assertThat(retrievedPublishResponse)
                            .as("trialIndex=%d", trialIndex)
                            .isEqualTo(PUBLISH_RESPONSE);
                    break;
                }

                default:
                    throw new IllegalStateException();

            }
        }

        // If we would have had a discrepancy between tags for each distinct
        // meter name, Prometheus meter registry would have thrown an error.
        // Coming this far indicates that no such discrepancy has occurred.

    }

    @Test
    public void test_pull_timeout_with_returnImmediateEnabled_true() {

        // Create Pub/Sub client.
        Duration pullTimeout = Duration.ofMillis(100);
        PubsubClientConfig clientConfig = PubsubClientConfig
                .builder()
                .setBaseUrl(serverMockRule.baseUrl())
                .setPullTimeout(pullTimeout)
                .build();
        PubsubAccessTokenCache accessTokenCache = PubsubAccessTokenCacheFixture.getInstance();
        PubsubClient client = PubsubClient
                .builder()
                .setConfig(clientConfig)
                .setAccessTokenCache(accessTokenCache)
                .build();

        // Stub pull response.
        String pullResponseJson = JacksonHelpers.writeValueAsString(PULL_RESPONSE);
        int responseDelayMillis = Math.toIntExact(pullTimeout.toMillis()) * 2;
        serverMockRule.addStubMapping(
                WireMock.stubFor(WireMock
                        .post(WireMock.urlEqualTo(PULL_REQUEST_RELATIVE_PATH))
                        .willReturn(WireMock
                                .aResponse()
                                .withFixedDelay(responseDelayMillis)
                                .withHeader(
                                        HttpHeaderNames.CONTENT_TYPE.toString(),
                                        HttpHeaderValues.APPLICATION_JSON.toString())
                                .withBody(pullResponseJson))));

        // Verify the timeout exception.
        Assertions
                .assertThatThrownBy(() -> client
                        .pull(PROJECT_NAME,
                                SUBSCRIPTION_NAME,
                                // Note that the PubsubPullRequest#returnImmediateEnabled
                                // is ignored in the stub mapping. Hence, any any
                                // serializable request payload would do the work here.
                                PULL_REQUEST)
                        .block(Duration.ofSeconds(1)))
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining("Did not observe any item or terminal signal within");

    }

    @Test
    public void test_pull_timeout_with_returnImmediateEnabled_false() {

        // Create Pub/Sub client.
        Duration pullTimeout = Duration.ZERO;
        PubsubClientConfig clientConfig = PubsubClientConfig
                .builder()
                .setBaseUrl(serverMockRule.baseUrl())
                .setPullTimeout(pullTimeout)
                .build();
        PubsubAccessTokenCache accessTokenCache = PubsubAccessTokenCacheFixture.getInstance();
        PubsubClient client = PubsubClient
                .builder()
                .setConfig(clientConfig)
                .setAccessTokenCache(accessTokenCache)
                .build();

        // Stub pull response.
        String pullResponseJson = JacksonHelpers.writeValueAsString(PULL_RESPONSE);
        int responseDelayMillis = 100;
        serverMockRule.addStubMapping(
                WireMock.stubFor(WireMock
                        .post(WireMock.urlEqualTo(PULL_REQUEST_RELATIVE_PATH))
                        .willReturn(WireMock
                                .aResponse()
                                .withFixedDelay(responseDelayMillis)
                                .withHeader(
                                        HttpHeaderNames.CONTENT_TYPE.toString(),
                                        HttpHeaderValues.APPLICATION_JSON.toString())
                                .withBody(pullResponseJson))));

        // Verify the timeout exception.
        @Nullable PubsubPullResponse pullResponse = client
                .pull(PROJECT_NAME,
                        SUBSCRIPTION_NAME,
                        // Note that the PubsubPullRequest#returnImmediateEnabled
                        // is ignored in the stub mapping. Hence, any any
                        // serializable request payload would do the work here.
                        PULL_REQUEST)
                .block(Duration.ofSeconds(1));
        Assertions.assertThat(pullResponse).isEqualTo(PULL_RESPONSE);

    }

}
