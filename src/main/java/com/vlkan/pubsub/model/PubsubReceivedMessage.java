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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.vlkan.pubsub.jackson.JacksonBase64EncodedStringDeserializer;
import com.vlkan.pubsub.jackson.JacksonInstantDeserializer;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * Jackson-deserializable Pub/Sub received message model.
 */
public class PubsubReceivedMessage {

    private final Instant publishInstant;

    private final String id;

    private final byte[] payload;

    @JsonCreator
    public PubsubReceivedMessage(
            @JsonProperty(value = "publishTime", required = true)
            @JsonDeserialize(using = JacksonInstantDeserializer.class)
                    Instant publishInstant,
            @JsonProperty(value = "messageId", required = true)
                    String id,
            @JsonProperty(value = "data", required = true)
            @JsonDeserialize(using = JacksonBase64EncodedStringDeserializer.class)
                    byte[] payload) {
        this.publishInstant = Objects.requireNonNull(publishInstant, "publishInstant");
        this.id = Objects.requireNonNull(id, "id");
        this.payload = Objects.requireNonNull(payload, "payload");
    }

    public Instant getPublishInstant() {
        return publishInstant;
    }

    public String getId() {
        return id;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubReceivedMessage that = (PubsubReceivedMessage) object;
        return Objects.equals(publishInstant, that.publishInstant) &&
                Objects.equals(id, that.id) &&
                Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(publishInstant, id);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "PubsubReceivedMessage{" +
                "id=" + id +
                ", publishInstant=" + publishInstant +
                '}';
    }

}
