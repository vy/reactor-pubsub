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
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.function.Supplier;

public class PubsubClient {

    private static final class DefaultObjectMapperHolder {

        private static final ObjectMapper INSTANCE = new ObjectMapper()
                // To allow backward-compatible future enhancements in the
                // protocol, disable failure on unknown properties.
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    }

    public static ObjectMapper getDefaultObjectMapper() {
        return DefaultObjectMapperHolder.INSTANCE;
    }

    private static final class DefaultHttpClientHolder {

        private static final HttpClient INSTANCE = HttpClient.create();

    }

    public static HttpClient getDefaultHttpClient() {
        return DefaultHttpClientHolder.INSTANCE;
    }

    private static final class DefaultInstanceHolder {

        private static final PubsubClient INSTANCE = PubsubClient.builder().build();

    }

    public static PubsubClient getDefaultInstance() {
        return DefaultInstanceHolder.INSTANCE;
    }

    private final PubsubClientConfig config;

    private final ObjectMapper objectMapper;

    private final PubsubAccessTokenCache accessTokenCache;

    private final HttpClient httpClient;

    @Nullable
    private final MeterRegistry meterRegistry;

    @Nullable
    private final Map<String, Timer> timerByRequestUrl;

    @Nullable
    private final Map<String, Counter> counterByRequestUrl;

    private PubsubClient(Builder builder) {
        this.config = builder.config;
        this.objectMapper = builder.objectMapper;
        this.accessTokenCache = builder.accessTokenCache;
        this.httpClient = builder.httpClient;
        if (builder.meterRegistry == null) {
            this.meterRegistry = null;
            this.timerByRequestUrl = null;
            this.counterByRequestUrl = null;
        } else {
            this.meterRegistry = builder.meterRegistry;
            this.timerByRequestUrl = Collections.synchronizedMap(new WeakHashMap<>());
            this.counterByRequestUrl = Collections.synchronizedMap(new WeakHashMap<>());
        }
    }

    Mono<PubsubPullResponse> pull(String projectName, String subscriptionName, PubsubPullRequest pullRequest) {
        String requestUrl = String.format(
                "%s/v1/projects/%s/subscriptions/%s:pull",
                config.getBaseUrl(), projectName, subscriptionName);
        Mono<PubsubPullResponse> pullResponseMono = meterRegistry == null
                ? executeRequest(requestUrl, pullRequest, PubsubPullResponse.class, config.getPullTimeout())
                : pullMeasured(projectName, subscriptionName, pullRequest, requestUrl);
        return pullResponseMono.checkpoint(requestUrl);
    }

    private Mono<PubsubPullResponse> pullMeasured(
            String projectName,
            String subscriptionName,
            PubsubPullRequest pullRequest,
            String requestUrl) {
        Supplier<String[]> tagSupplier = createMeterTagSupplier(
                "operation", "pull",
                "projectName", projectName,
                "subscriptionName", subscriptionName);
        return executeRequest(requestUrl, pullRequest, PubsubPullResponse.class, config.getPullTimeout())
                .transform(mono -> MicrometerHelpers.measureLatency(
                        meterRegistry,
                        config.getMeterName(),
                        timerByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        mono))
                .transform(mono -> MicrometerHelpers.measureCount(
                        meterRegistry,
                        config.getMeterName(),
                        counterByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        pullResponse -> pullResponse.getReceivedAckableMessages().size(),
                        mono));
    }

    Mono<Void> ack(String projectName, String subscriptionName, PubsubAckRequest ackRequest) {
        String requestUrl = String.format(
                "%s/v1/projects/%s/subscriptions/%s:acknowledge",
                config.getBaseUrl(), projectName, subscriptionName);
        Mono<Void> ackResponseMono = meterRegistry == null
                ? executeRequest(requestUrl, ackRequest, Void.class, config.getAckTimeout())
                : ackMeasured(projectName, subscriptionName, ackRequest, requestUrl);
        return ackResponseMono.checkpoint(requestUrl);
    }

    private Mono<Void> ackMeasured(
            String projectName,
            String subscriptionName,
            PubsubAckRequest ackRequest,
            String requestUrl) {
        Supplier<String[]> tagSupplier = createMeterTagSupplier(
                "operation", "publish",
                "projectName", projectName,
                "subscriptionName", subscriptionName);
        return executeRequest(requestUrl, ackRequest, Void.class, config.getAckTimeout())
                .transform(mono -> MicrometerHelpers.measureLatency(
                        meterRegistry,
                        config.getMeterName(),
                        timerByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        mono))
                .transform(mono -> MicrometerHelpers.measureCount(
                        meterRegistry,
                        config.getMeterName(),
                        counterByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        ignored -> ackRequest.getAckIds().size(),
                        mono));
    }

    Mono<PubsubPublishResponse> publish(String projectName, String topicName, PubsubPublishRequest publishRequest) {
        String requestUrl = String.format(
                "%s/v1/projects/%s/topics/%s:publish",
                config.getBaseUrl(), projectName, topicName);
        Mono<PubsubPublishResponse> publishResponseMono = meterRegistry == null
                ? executeRequest(requestUrl, publishRequest, PubsubPublishResponse.class, config.getPublishTimeout())
                : publishMeasured(projectName, topicName, publishRequest, requestUrl);
        return publishResponseMono.checkpoint(requestUrl);
    }

    private Mono<PubsubPublishResponse> publishMeasured(
            String projectName,
            String topicName,
            PubsubPublishRequest publishRequest,
            String requestUrl) {
        Supplier<String[]> tagSupplier = createMeterTagSupplier(
                "operation", "publish",
                "projectName", projectName,
                "topicName", topicName);
        return executeRequest(requestUrl, publishRequest, PubsubPublishResponse.class, config.getPublishTimeout())
                .transform(mono -> MicrometerHelpers.measureLatency(
                        meterRegistry,
                        config.getMeterName(),
                        timerByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        mono))
                .transform(mono -> MicrometerHelpers.measureCount(
                        meterRegistry,
                        config.getMeterName(),
                        counterByRequestUrl,
                        requestUrl,
                        tagSupplier,
                        ignored -> publishRequest.getMessages().size(),
                        mono));
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
                                    .asByteArray()
                                    .flatMap(responsePayloadBytes -> {
                                        @Nullable T responsePayload = deserializeResponsePayload(responsePayloadBytes, responseClass);
                                        return responsePayload != null
                                                ? Mono.just(responsePayload)
                                                : Mono.empty();
                                    })
                                    .checkpoint("deserializeResponsePayload");
                        })
                        .timeout(timeout));
    }

    private Supplier<String[]> createMeterTagSupplier(String... extensionTags) {
        return () -> {
            Map<String, String> commonTags = config.getMeterTags();
            String[] tags = new String[extensionTags.length + commonTags.size() * 2];
            System.arraycopy(extensionTags, 0, tags, 0, extensionTags.length);
            int[] i = {extensionTags.length};
            commonTags.forEach((tagName, tagValue) -> {
                tags[i[0]++] = tagName;
                tags[i[0]++] = tagValue;
            });
            return tags;
        };
    }

    private static boolean is2xxSuccessful(HttpResponseStatus status) {
        int statusCode = status.code();
        int statusCodeSeries = statusCode / 100;
        return statusCodeSeries == 2;
    }

    private ByteBuf serializeRequestPayload(Object requestPayload) {
        try {
            byte[] requestPayloadJsonBytes = objectMapper.writeValueAsBytes(requestPayload);
            return Unpooled.copiedBuffer(requestPayloadJsonBytes);
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

    @Nullable
    private <T> T deserializeResponsePayload(byte[] responsePayloadBytes, Class<T> responsePayloadClass) {
        try {
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

        private ObjectMapper objectMapper;

        private HttpClient httpClient;

        private PubsubAccessTokenCache accessTokenCache;

        @Nullable
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

        public Builder setHttpClient(HttpClient httpClient) {
            this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
            return this;
        }

        public Builder setAccessTokenCache(PubsubAccessTokenCache accessTokenCache) {
            this.accessTokenCache = Objects.requireNonNull(accessTokenCache, "accessTokenCache");
            return this;
        }

        public Builder setMeterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
            return this;
        }

        public PubsubClient build() {
            if (objectMapper == null) {
                objectMapper = getDefaultObjectMapper();
            }
            if (httpClient == null) {
                httpClient = getDefaultHttpClient();
            }
            if (accessTokenCache == null) {
                accessTokenCache = PubsubAccessTokenCache.getDefaultInstance();
            }
            return new PubsubClient(this);
        }

    }

}
