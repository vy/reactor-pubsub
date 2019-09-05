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

import com.vlkan.pubsub.model.PubsubAckRequest;
import com.vlkan.pubsub.model.PubsubPullRequest;
import com.vlkan.pubsub.model.PubsubPullResponse;
import com.vlkan.pubsub.model.PubsubReceivedAckableMessage;
import com.vlkan.pubsub.ratelimiter.StagedRateLimiter;
import com.vlkan.pubsub.util.FluxHelpers;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class PubsubSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubsubSubscriber.class);

    private final PubsubSubscriberConfig config;

    private final PubsubClient client;

    @Nullable
    private final Scheduler scheduler;

    private final PullResponseConsumer pullResponseConsumer;

    private final PubsubPullRequest pullRequest;

    private final StagedRateLimiter rateLimiter;

    private final DistributionSummary permitWaitPeriodDistributions;

    @FunctionalInterface
    public interface PullResponseConsumer {

        Mono<PubsubAckRequest> accept(PubsubPullResponse pullResponse);

    }

    private PubsubSubscriber(Builder builder) {
        this.config = builder.config;
        this.client = builder.client;
        this.scheduler = builder.scheduler;
        this.pullResponseConsumer = builder.pullResponseConsumer;
        this.pullRequest = new PubsubPullRequest(true, config.getPullBufferSize());
        String rateLimiterName = String.format("%s/%s", config.getProjectName(), config.getSubscriptionName());
        this.rateLimiter = StagedRateLimiter.of(rateLimiterName, config.getRateLimiterSpec());
        this.permitWaitPeriodDistributions = DistributionSummary
                .builder(config.getMeterName())
                .register(builder.meterRegistry);
    }

    public PubsubSubscriberConfig getConfig() {
        return config;
    }

    public PubsubClient getClient() {
        return client;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public PullResponseConsumer getPullResponseConsumer() {
        return pullResponseConsumer;
    }

    public Flux<PubsubSubscriberConsumption> start() {
        Flux<PubsubSubscriberConsumption> flux = Flux
                .just(new AtomicBoolean(false))
                .flatMap(emptyBatchReceivedRef -> FluxHelpers
                        .infiniteRange(BigDecimal.ZERO)
                        .concatMap(requestIndex -> {
                            boolean delayEnabled = emptyBatchReceivedRef.get();
                            if (delayEnabled) {
                                return scheduler == null
                                        ? Mono.just(requestIndex).delayElement(config.getPullPeriod())
                                        : Mono.just(requestIndex).delayElement(config.getPullPeriod(), scheduler);
                            } else {
                                return Mono.just(requestIndex);
                            }
                        })
                        .flatMap(
                                requestIndex -> pullAndConsumeAndAck(emptyBatchReceivedRef, requestIndex),
                                config.getPullConcurrency()));
        return scheduler == null
                ? flux
                : flux.subscribeOn(scheduler);
    }

    private Mono<PubsubSubscriberConsumption> pullAndConsumeAndAck(
            AtomicBoolean emptyBatchReceivedRef,
            BigDecimal requestIndex) {
        return client
                .pull(config.getProjectName(), config.getSubscriptionName(), pullRequest)
                .filter(pullResponse -> {
                    boolean emptyBatchReceived = pullResponse.getReceivedAckableMessages().isEmpty();
                    emptyBatchReceivedRef.set(emptyBatchReceived);
                    return !emptyBatchReceived;
                })
                .flatMap(this::consumeAndAck)
                .transform(this::rateLimit)
                // Warning! onErrorResume() should be placed after transform()
                // calls so the latter can observe errors. [vyazici]
                .onErrorResume(error -> {
                    String errorMessage = String.format(
                            "failed consuming message (projectName=%s, subscriptionName=%s, requestIndex=%s)",
                            config.getProjectName(), config.getSubscriptionName(), requestIndex);
                    LOGGER.error(errorMessage, error);
                    return Mono.empty();
                });
    }

    private Mono<PubsubSubscriberConsumption> consumeAndAck(PubsubPullResponse pullResponse) {
        return Mono
                .defer(() -> {

                    // Discard empty batches.
                    List<PubsubReceivedAckableMessage> receivedAckableMessages = pullResponse.getReceivedAckableMessages();
                    int receivedAckableMessageCount = receivedAckableMessages.size();
                    if (receivedAckableMessageCount < 1) {
                        return Mono.empty();
                    }

                    // Consume and ack messages.
                    return pullResponseConsumer
                            .accept(pullResponse)
                            .checkpoint("pullResponseConsumer")
                            .flatMap(ackRequest -> client
                                    .ack(config.getProjectName(), config.getSubscriptionName(), ackRequest)
                                    .thenReturn(new PubsubSubscriberConsumption(pullResponse, ackRequest)));

                })
                .checkpoint("consumeAndAck");
    }

    private <V> Mono<V> rateLimit(Mono<V> mono) {
        return mono
                .flatMap(value -> {
                    long permitWaitPeriodNanos = rateLimiter.nextPermitWaitPeriodNanos();
                    permitWaitPeriodDistributions.record(permitWaitPeriodNanos);
                    if (permitWaitPeriodNanos > 0L) {
                        return scheduler == null
                                ? Mono.just(value).delayElement(Duration.ofNanos(permitWaitPeriodNanos))
                                : Mono.just(value).delayElement(Duration.ofNanos(permitWaitPeriodNanos), scheduler);
                    } else {
                        return Mono.just(value);
                    }
                })
                .doOnError(ignored -> rateLimiter.acknowledgeFailure());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private PubsubSubscriberConfig config;

        private PubsubClient client;

        @Nullable
        private Scheduler scheduler;

        private PullResponseConsumer pullResponseConsumer;

        private MeterRegistry meterRegistry;

        private Builder() {}

        public Builder setConfig(PubsubSubscriberConfig config) {
            this.config = Objects.requireNonNull(config, "config");
            return this;
        }

        public Builder setClient(PubsubClient client) {
            this.client = Objects.requireNonNull(client, "client");
            return this;
        }

        public Builder setScheduler(Scheduler scheduler) {
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
            return this;
        }

        public Builder setPullResponseConsumer(PullResponseConsumer pullResponseConsumer) {
            this.pullResponseConsumer = Objects.requireNonNull(pullResponseConsumer, "pullResponseConsumer");
            return this;
        }

        public Builder setMeterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
            return this;
        }

        public PubsubSubscriber build() {
            Objects.requireNonNull(config, "config");
            Objects.requireNonNull(client, "client");
            Objects.requireNonNull(pullResponseConsumer, "pullResponseConsumer");
            Objects.requireNonNull(meterRegistry, "meterRegistry");
            return new PubsubSubscriber(this);
        }

    }

}
