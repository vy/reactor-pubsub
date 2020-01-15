/*
 * Copyright 2019-2020 Volkan Yazıcı
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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.vlkan.pubsub.jackson.JacksonBase64EncodedStringDeserializer;
import com.vlkan.pubsub.jackson.JacksonBase64EncodedStringSerializer;
import com.vlkan.pubsub.jackson.JacksonInstantDeserializer;
import com.vlkan.pubsub.jackson.JacksonInstantSerializer;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Jackson-(de)serializable Pub/Sub received message model.
 */
public class PubsubReceivedMessageEmbedding {

    enum JsonFieldName {;

        static final String PUBLISH_INSTANT = "publishTime";

        static final String ID = "messageId";

        static final String PAYLOAD = "data";

        static final String ATTRIBUTES = "attributes";

    }

    @JsonProperty(JsonFieldName.PUBLISH_INSTANT)
    @JsonSerialize(using = JacksonInstantSerializer.class)
    private final Instant publishInstant;

    @JsonProperty(JsonFieldName.ID)
    private final String id;

    @JsonProperty(JsonFieldName.PAYLOAD)
    @JsonSerialize(using = JacksonBase64EncodedStringSerializer.class)
    private final byte[] payload;

    @JsonProperty(JsonFieldName.ATTRIBUTES)
    private final Map<String, String> attributes;

    @JsonCreator
    public PubsubReceivedMessageEmbedding(
            @JsonProperty(value = JsonFieldName.PUBLISH_INSTANT, required = true)
            @JsonDeserialize(using = JacksonInstantDeserializer.class)
                    Instant publishInstant,
            @JsonProperty(value = JsonFieldName.ID, required = true)
                    String id,
            @JsonProperty(value = JsonFieldName.PAYLOAD, required = true)
            @JsonDeserialize(using = JacksonBase64EncodedStringDeserializer.class)
                    byte[] payload,
            @JsonProperty(JsonFieldName.ATTRIBUTES)
                    Map<String, String> attributes) {
        this.publishInstant = Objects.requireNonNull(publishInstant, "publishInstant");
        this.id = Objects.requireNonNull(id, "id");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.attributes = attributes != null
                ? attributes
                : Collections.emptyMap();
        if (payload.length == 0 && this.attributes.isEmpty()) {
            throw new IllegalArgumentException("both payload and attributes cannot be null");
        }
    }

    Instant getPublishInstant() {
        return publishInstant;
    }

    String getId() {
        return id;
    }

    byte[] getPayload() {
        return payload;
    }

    Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubReceivedMessageEmbedding that = (PubsubReceivedMessageEmbedding) object;
        return Objects.equals(publishInstant, that.publishInstant) &&
                Objects.equals(id, that.id) &&
                Arrays.equals(payload, that.payload) &&
                Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(publishInstant, id, attributes);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "PubsubReceivedMessageEmbedding{" +
                "id=" + id +
                ", publishInstant=" + publishInstant +
                ", attributes=" + attributes +
                '}';
    }

}
