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

package com.vlkan.pubsub.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Jackson-serializable Pub/Sub publish request model.
 */
public class PubsubPublishRequest {

    enum JsonFieldName {;

        static final String MESSAGES = "messages";

    }

    @JsonProperty(JsonFieldName.MESSAGES)
    private final List<PubsubDraftedMessage> messages;

    public PubsubPublishRequest(List<PubsubDraftedMessage> messages) {
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("empty messages");
        }
        this.messages = messages;
    }

    public List<PubsubDraftedMessage> getMessages() {
        return messages;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubPublishRequest that = (PubsubPublishRequest) object;
        return Objects.equals(messages, that.messages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messages);
    }

    @Override
    public String toString() {
        int messageCount = messages.size();
        return "PubsubPublishRequest{" +
                "messageCount=" + messageCount +
                '}';
    }

}
