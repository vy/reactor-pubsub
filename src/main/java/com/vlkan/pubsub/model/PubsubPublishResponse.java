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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Jackson-(de)serializable Pub/Sub publish response model.
 */
public class PubsubPublishResponse {

    @JsonProperty
    private final List<String> messageIds;

    @JsonCreator
    public PubsubPublishResponse(@JsonProperty(value = "messageIds", required = true) List<String> messageIds) {
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
        String formattedMessageIds = formatMessageIds();
        return "PubsubPublishResponse{" +
                "messageIds=" + formattedMessageIds +
                '}';
    }

    private String formatMessageIds() {
        int messageIdCount = messageIds.size();
        int maxFormattedMessageIdCount = 2;
        List<String> visibleMessageIds = messageIds;
        if (messageIdCount > maxFormattedMessageIdCount) {
            visibleMessageIds = Stream
                    .concat(messageIds.stream().limit(maxFormattedMessageIdCount),
                            Stream.of(String.format("<%d more>", messageIdCount - maxFormattedMessageIdCount)))
                    .collect(Collectors.toList());
        }
        return String.valueOf(visibleMessageIds);
    }

}
