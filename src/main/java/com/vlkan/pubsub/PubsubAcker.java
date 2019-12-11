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
import com.vlkan.pubsub.model.PubsubPullResponse;
import com.vlkan.pubsub.model.PubsubReceivedMessage;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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

    public Mono<Void> ackPullResponse(PubsubPullResponse pullResponse) {
        Objects.requireNonNull(pullResponse, "pullResponse");
        return ackMessages(pullResponse.getReceivedMessages());
    }

    public Mono<Void> ackMessages(List<PubsubReceivedMessage> messages) {
        Objects.requireNonNull(messages, "messages");
        List<String> ackIds = messages
                .stream()
                .map(PubsubReceivedMessage::getAckId)
                .collect(Collectors.toList());
        return ackIds(ackIds);
    }

    public Mono<Void> ackMessage(PubsubReceivedMessage message) {
        Objects.requireNonNull(message, "message");
        String ackId = message.getAckId();
        return ackId(ackId);
    }

    public Mono<Void> ackIds(List<String> ackIds) {
        Objects.requireNonNull(ackIds, "ackIds");
        PubsubAckRequest ackRequest = new PubsubAckRequest(ackIds);
        return ack(ackRequest);
    }

    public Mono<Void> ackId(String ackId) {
        Objects.requireNonNull(ackId, "ackId");
        List<String> ackIds = Collections.singletonList(ackId);
        PubsubAckRequest ackRequest = new PubsubAckRequest(ackIds);
        return ack(ackRequest);
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
                client = PubsubClient.getDefaultInstance();
            }
            return new PubsubAcker(this);
        }

    }

}
