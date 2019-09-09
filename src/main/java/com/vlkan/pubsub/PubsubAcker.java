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
import reactor.core.publisher.Mono;

import java.util.Objects;

public class PubsubAcker {

    private final PubsubAckerConfig config;

    private final PubsubClient client;

    private PubsubAcker(Builder builder) {
        this.config = builder.config;
        this.client = builder.client;
    }

    public PubsubAckerConfig getConfig() {
        return config;
    }

    public PubsubClient getClient() {
        return client;
    }

    public Mono<Void> ack(PubsubAckRequest ackRequest) {
        Objects.requireNonNull(ackRequest, "ackRequest");
        return client.ack(config.getProjectName(), config.getSubscriptionName(), ackRequest);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private PubsubAckerConfig config;

        private PubsubClient client;

        private Builder() {}

        public Builder setConfig(PubsubAckerConfig config) {
            this.config = Objects.requireNonNull(config, "config");
            return this;
        }

        public Builder setClient(PubsubClient client) {
            this.client = Objects.requireNonNull(client, "client");
            return this;
        }

        public PubsubAcker build() {
            Objects.requireNonNull(config, "config");
            if (client == null) {
                client = PubsubClient.DEFAULT_CLIENT_SUPPLIER.get();
            }
            return new PubsubAcker(this);
        }

    }

}
