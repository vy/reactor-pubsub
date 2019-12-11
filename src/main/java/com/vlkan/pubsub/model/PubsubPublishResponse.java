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
import com.vlkan.pubsub.util.CollectionHelpers;

import java.util.List;
import java.util.Objects;

/**
 * Jackson-(de)serializable Pub/Sub publish response model.
 */
public class PubsubPublishResponse {

    enum JsonFieldName {;

        static final String MESSAGE_IDS = "messageIds";

    }

    @JsonProperty(JsonFieldName.MESSAGE_IDS)
    private final List<String> messageIds;

    @JsonCreator
    public PubsubPublishResponse(
            @JsonProperty(value = JsonFieldName.MESSAGE_IDS, required = true)
                    List<String> messageIds) {
        Objects.requireNonNull(messageIds, "messageIds");
        if (messageIds.isEmpty()) {
            throw new IllegalArgumentException("emtpy messageIds");
        }
        this.messageIds = messageIds;
    }

    public List<String> getMessageIds() {
        return messageIds;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubPublishResponse that = (PubsubPublishResponse) object;
        return Objects.equals(messageIds, that.messageIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageIds);
    }

    @Override
    public String toString() {
        String formattedMessageIds = CollectionHelpers.limitedFormat(messageIds, 2);
        return "PubsubPublishResponse{" +
                "messageIds=" + formattedMessageIds +
                '}';
    }

}
