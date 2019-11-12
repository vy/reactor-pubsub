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

import java.util.Objects;

/**
 * Jackson-(de)serializable Pub/Sub received (ack'able) message model.
 */
public class PubsubReceivedAckableMessage {

    @JsonProperty(value = "ackId", required = true)
    private final String ackId;

    @JsonProperty(value = "message", required = true)
    private final PubsubReceivedMessage message;

    @JsonCreator
    public PubsubReceivedAckableMessage(
            @JsonProperty(value = "ackId", required = true) String ackId,
            @JsonProperty(value = "message", required = true) PubsubReceivedMessage message) {
        this.ackId = Objects.requireNonNull(ackId, "ackId");
        this.message = Objects.requireNonNull(message, "message");
    }

    public String getAckId() {
        return ackId;
    }

    public PubsubReceivedMessage getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubReceivedAckableMessage that = (PubsubReceivedAckableMessage) object;
        return Objects.equals(ackId, that.ackId) &&
                Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ackId, message);
    }

    @Override
    public String toString() {
        return "PubsubReceivedAckableMessage{" +
                "ackId=" + ackId +
                '}';
    }

}
