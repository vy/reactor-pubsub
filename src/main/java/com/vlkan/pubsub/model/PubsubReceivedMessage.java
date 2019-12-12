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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Jackson-(de)serializable Pub/Sub received message model.
 */
public class PubsubReceivedMessage {

    enum JsonFieldName {;

        static final String ACK_ID = "ackId";

        static final String EMBEDDING = "message";

    }

    @JsonProperty(value = JsonFieldName.ACK_ID)
    private final String ackId;

    @JsonProperty(value = JsonFieldName.EMBEDDING)
    private final PubsubReceivedMessageEmbedding embedding;

    @JsonCreator
    public PubsubReceivedMessage(
            @JsonProperty(value = JsonFieldName.ACK_ID, required = true) String ackId,
            @JsonProperty(value = JsonFieldName.EMBEDDING, required = true) PubsubReceivedMessageEmbedding embedding) {
        this.ackId = Objects.requireNonNull(ackId, "ackId");
        this.embedding = Objects.requireNonNull(embedding, "embedding");
    }

    /**
     * ID to used for acknowledging the received message.
     */
    public String getAckId() {
        return ackId;
    }

    @JsonIgnore
    public Instant getPublishInstant() {
        return embedding.getPublishInstant();
    }

    /**
     * ID assigned by the server when the message is published.
     * Guaranteed to be unique within the topic.
     */
    @JsonIgnore
    public String getId() {
        return embedding.getId();
    }

    @JsonIgnore
    public byte[] getPayload() {
        return embedding.getPayload();
    }

    @JsonIgnore
    public Map<String, String> getAttributes() {
        return embedding.getAttributes();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubReceivedMessage that = (PubsubReceivedMessage) object;
        return Objects.equals(ackId, that.ackId) &&
                Objects.equals(embedding, that.embedding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ackId, embedding);
    }

    @Override
    public String toString() {
        return "PubsubReceivedMessage{" +
                "ackId=" + ackId +
                ", id=" + embedding.getId() +
                ", publishInstant=" + embedding.getPublishInstant() +
                ", attributes=" + embedding.getPublishInstant() +
                '}';
    }

}
