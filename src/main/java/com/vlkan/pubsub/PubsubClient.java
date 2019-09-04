/*
 * Copyright 2019 Volkan Yazıcı
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vlkan.pubsub.model.*;
import com.vlkan.pubsub.util.MicrometerHelpers;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.function.Supplier;

public class PubsubClient {

    public static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper()
            // To allow backward-compatible future enhancements in the
            // protocol, disable failure on unknown properties.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final PubsubClientConfig config;

    private final ObjectMapper objectMapper;

    private final PubsubAccessTokenCache accessTokenCache;

    private final HttpClient httpClient;

    private final MeterRegistry meterRegistry;

    private final Map<String, Timer> timerByRequestUrl = new WeakHashMap<>();

    private final Map<String, Counter> counterByRequestUrl = new WeakHashMap<>();

    private PubsubClient(Builder builder) {
        this.config = builder.config;
        this.objectMapper = builder.objectMapper;
        this.accessTokenCache = builder.accessTokenCache;
        this.httpClient = builder.httpClient;
        this.meterRegistry = builder.meterRegistry;
    }

    Mono<PubsubPullResponse> pull(String projectName, String subscriptionName, PubsubPullRequest pullRequest) {
        String requestUrl = String.format(
                "%s/v1/projects/%s/subscriptions/%s:pull",
                config.getBaseUrl(), projectName, subscriptionName);
        Supplier<String[]> tagSupplier = () -> new String[]{
                "operation", "pull",
                "projectName", projectName,
                "subscriptionName", subscriptionName
        };
        return executeRequest(requestUrl, pullRequest, PubsubPullResponse.class, config.getPullTimeout())
                .transform(mono -> MicrometerHelpers.measureLatency(
                        meterRegistry,
                        timerByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        mono))
                .transform(mono -> MicrometerHelpers.measureCount(
                        meterRegistry,
                        counterByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        pullResponse -> pullResponse.getReceivedAckableMessages().size(),
                        mono))
                .checkpoint(requestUrl);
    }

    Mono<Void> ack(String projectName, String subscriptionName, PubsubAckRequest ackRequest) {
        String requestUrl = String.format(
                "%s/v1/projects/%s/subscriptions/%s:acknowledge",
                config.getBaseUrl(), projectName, subscriptionName);
        return executeRequest(requestUrl, ackRequest, Void.class, config.getAckTimeout())
                .transform(mono -> MicrometerHelpers.measureLatency(
                        meterRegistry,
                        timerByRequestUrl,
                        requestUrl,
                        () -> new String[]{
                                "operation", "publish",
                                "projectName", projectName,
                                "subscriptionName", subscriptionName
                        },
                        mono))
                .transform(mono -> MicrometerHelpers.measureCount(
                        meterRegistry,
                        counterByRequestUrl,
                        requestUrl,
                        () -> new String[]{
                                "operation", "publish",
                                "projectName", projectName,
                                "subscriptionName", subscriptionName
                        },
                        ignored -> ackRequest.getAckIds().size(),
                        mono))
                .checkpoint(requestUrl)
                .then();
    }

    Mono<PubsubPublishResponse> publish(String projectName, String topicName, PubsubPublishRequest publishRequest) {
        String requestUrl = String.format(
                "%s/v1/projects/%s/topics/%s:publish",
                config.getBaseUrl(), projectName, topicName);
        Supplier<String[]> tagSupplier = () -> new String[]{
                "operation", "publish",
                "projectName", projectName,
                "topicName", topicName
        };
        return executeRequest(requestUrl, publishRequest, PubsubPublishResponse.class, config.getPublishTimeout())
                .transform(mono -> MicrometerHelpers.measureLatency(
                        meterRegistry,
                        timerByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        mono))
                .transform(mono -> MicrometerHelpers.measureCount(
                        meterRegistry,
                        counterByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        ignored -> publishRequest.getMessages().size(),
                        mono))
                .checkpoint(requestUrl);
    }

    private <T> Mono<T> executeRequest(String requestUrl, Object requestPayload, Class<T> responseClass, Duration timeout) {
        Mono<ByteBuf> requestPayloadByteBufMono = Mono
                .fromCallable(() -> serializeRequestPayload(requestPayload))
                .checkpoint("serializeRequestPayload");
        return Mono
                .fromCallable(() -> String.format("Bearer %s", accessTokenCache.getAccessToken()))
                .flatMap(authorizationHeaderValue -> httpClient
                        .headers(headers -> {
                            headers
                                    .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                    .set(HttpHeaderNames.AUTHORIZATION, authorizationHeaderValue);
                            @Nullable String userAgent = config.getUserAgent();
                            if (userAgent != null) {
                                headers.set(HttpHeaderNames.USER_AGENT, userAgent);
                            }
                        })
                        .post()
                        .uri(requestUrl)
                        .send(requestPayloadByteBufMono)
                        .responseSingle((response, responsePayloadByteBufMono) -> {
                            HttpResponseStatus responseStatus = response.status();
                            if (!is2xxSuccessful(responseStatus)) {
                                String message = String.format("unexpected response (responseStatus=%s)", responseStatus);
                                throw new RuntimeException(message);
                            }
                            return responsePayloadByteBufMono
                                    .map(responsePayloadByteBuf -> deserializeResponsePayload(responsePayloadByteBuf, responseClass))
                                    .checkpoint("deserializeResponsePayload");
                        })
                        .timeout(timeout));
    }

    private static boolean is2xxSuccessful(HttpResponseStatus status) {
        int statusCode = status.code();
        int statusCodeSeries = statusCode / 100;
        return statusCodeSeries == 2;
    }

    private ByteBuf serializeRequestPayload(Object requestPayload) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            objectMapper.writeValue(outputStream, requestPayload);
            return Unpooled.copiedBuffer(outputStream.toByteArray());
        } catch (IOException error) {
            @Nullable String requestPayloadClassName = requestPayload != null
                    ? requestPayload.getClass().getCanonicalName()
                    : null;
            String message = String.format(
                    "request payload serialization failure (requestPayloadClassName=%s)",
                    requestPayloadClassName);
            throw new RuntimeException(message, error);
        }
    }

    private <T> T deserializeResponsePayload(ByteBuf responsePayloadByteBuf, Class<T> responsePayloadClass) {
        try {
            byte[] responsePayloadBytes = responsePayloadByteBuf.array();
            return objectMapper.readValue(responsePayloadBytes, responsePayloadClass);
        } catch (IOException error) {
            String responsePayloadClassName = responsePayloadClass.getCanonicalName();
            String message = String.format(
                    "response payload deserialization failure (responsePayloadClassName=%s)",
                    responsePayloadClassName);
            throw new RuntimeException(message, error);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private PubsubClientConfig config = PubsubClientConfig.DEFAULT;

        private ObjectMapper objectMapper = DEFAULT_OBJECT_MAPPER;

        private PubsubAccessTokenCache accessTokenCache;

        private HttpClient httpClient;

        private MeterRegistry meterRegistry;

        private Builder() {}

        public Builder setConfig(PubsubClientConfig config) {
            Objects.requireNonNull(config, "config");
            this.config = config;
            return this;
        }

        public Builder setObjectMapper(ObjectMapper objectMapper) {
            this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
            return this;
        }

        public Builder setAccessTokenCache(PubsubAccessTokenCache accessTokenCache) {
            this.accessTokenCache = Objects.requireNonNull(accessTokenCache, "accessTokenCache");
            return this;
        }

        public Builder setHttpClient(HttpClient httpClient) {
            this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
            return this;
        }

        public Builder setMeterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
            return this;
        }

        public PubsubClient build() {
            Objects.requireNonNull(accessTokenCache, "accessTokenCache");
            Objects.requireNonNull(httpClient, "httpClient");
            Objects.requireNonNull(meterRegistry, "meterRegistry");
            return new PubsubClient(this);
        }

    }

}
