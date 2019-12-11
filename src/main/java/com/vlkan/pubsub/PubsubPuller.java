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

import com.vlkan.pubsub.model.PubsubPullRequest;
import com.vlkan.pubsub.model.PubsubPullResponse;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

public class PubsubPuller {

    private final PubsubPullerConfig config;

    private final PubsubClient client;

    @Nullable
    private final Scheduler scheduler;

    private final boolean immediateReturnEnabled;

    private final PubsubPullRequest pullRequest;

    private PubsubPuller(Builder builder) {
        this.config = builder.config;
        this.client = builder.client;
        this.scheduler = builder.scheduler;
        this.immediateReturnEnabled = !Duration.ZERO.equals(config.getPullPeriod());
        this.pullRequest = new PubsubPullRequest(immediateReturnEnabled, config.getPullBufferSize());
    }

    public PubsubPullerConfig getConfig() {
        return config;
    }

    public PubsubClient getClient() {
        return client;
    }

    @Nullable
    public Scheduler getScheduler() {
        return scheduler;
    }

    public Mono<PubsubPullResponse> pullOne() {
        return client
                .pull(config.getProjectName(), config.getSubscriptionName(), pullRequest)
                .filter(pullResponse -> !pullResponse.getReceivedMessages().isEmpty());
    }

    public Flux<PubsubPullResponse> pullAll() {
        return client
                .pull(config.getProjectName(), config.getSubscriptionName(), pullRequest)
                .filter(pullResponse -> !pullResponse.getReceivedMessages().isEmpty())
                .transform(this::delayEmptyPullsIfNecessary)
                .repeat()
                .checkpoint("pullAll");
    }

    private Mono<PubsubPullResponse> delayEmptyPullsIfNecessary(Mono<PubsubPullResponse> pullResponseMono) {
        if (!immediateReturnEnabled) {
            return pullResponseMono;
        }
        Function<Flux<Long>, ? extends Publisher<?>> repeatFactory = scheduler == null
                ? flux -> flux.delayElements(config.getPullPeriod())
                : flux -> flux.delayElements(config.getPullPeriod(), scheduler);
        return pullResponseMono.repeatWhenEmpty(repeatFactory);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private PubsubPullerConfig config;

        private PubsubClient client;

        @Nullable
        private Scheduler scheduler;

        private Builder() {}

        public Builder setConfig(PubsubPullerConfig config) {
            this.config = Objects.requireNonNull(config, "config");
            return this;
        }

        public Builder setClient(PubsubClient client) {
            this.client = Objects.requireNonNull(client, "client");
            return this;
        }

        public Builder setScheduler(@Nullable Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public PubsubPuller build() {
            Objects.requireNonNull(config, "config");
            if (client == null) {
                client = PubsubClient.getDefaultInstance();
            }
            return new PubsubPuller(this);
        }

    }

}
