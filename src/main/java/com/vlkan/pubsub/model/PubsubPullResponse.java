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

    enum JsonFieldName {;

        static final String RECEIVED_MESSAGES = "receivedMessages";

    }

    @JsonProperty(JsonFieldName.RECEIVED_MESSAGES)
    private final List<PubsubReceivedMessage> receivedMessages;

    @JsonCreator
    public PubsubPullResponse(
            @JsonProperty(JsonFieldName.RECEIVED_MESSAGES)
                    List<PubsubReceivedMessage> receivedMessages) {
        this.receivedMessages = receivedMessages != null
                ? receivedMessages
                : Collections.emptyList();
    }

    public List<PubsubReceivedMessage> getReceivedMessages() {
        return receivedMessages;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubPullResponse that = (PubsubPullResponse) object;
        return Objects.equals(receivedMessages, that.receivedMessages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(receivedMessages);
    }

    @Override
    public String toString() {
        int messageCount = receivedMessages.size();
        return "PubsubPullResponse{" +
                "messageCount=" + messageCount +
                '}';
    }

}
