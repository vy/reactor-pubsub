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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.vlkan.pubsub.jackson.JacksonBase64EncodedStringSerializer;

import java.util.Arrays;
import java.util.Objects;

/**
 * Jackson-serializable Pub/Sub outbound message draft model.
 */
public class PubsubDraftedMessage {

    @JsonProperty("data")
    @JsonSerialize(using = JacksonBase64EncodedStringSerializer.class)
    private final byte[] payload;

    public PubsubDraftedMessage(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubDraftedMessage that = (PubsubDraftedMessage) object;
        return Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        int payloadLength = payload.length;
        return "PubsubDraftedMessage{" +
                "payloadLength=" + payloadLength +
                '}';
    }

}
