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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Jackson-deserializable Pub/Sub pull response model.
 */
public class PubsubPullResponse {

    @JsonProperty(value = "receivedMessages")
    private final List<PubsubReceivedAckableMessage> receivedAckableMessages;

    @JsonCreator
    public PubsubPullResponse(
            @JsonProperty(value = "receivedMessages") List<PubsubReceivedAckableMessage> receivedAckableMessages) {
        this.receivedAckableMessages = receivedAckableMessages != null
                ? receivedAckableMessages
                : Collections.emptyList();
    }

    public List<PubsubReceivedAckableMessage> getReceivedAckableMessages() {
        return receivedAckableMessages;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubPullResponse that = (PubsubPullResponse) object;
        return Objects.equals(receivedAckableMessages, that.receivedAckableMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(receivedAckableMessages);
    }

    @Override
    public String toString() {
        int messageCount = receivedAckableMessages.size();
        return "PubsubPullResponse{" +
                "messageCount=" + messageCount +
                '}';
    }

}
