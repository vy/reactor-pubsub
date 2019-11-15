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

import java.util.Objects;

/**
 * Jackson-serializable Pub/Sub pull request model.
 */
public class PubsubPullRequest {

    enum JsonFieldName {;

        static final String IMMEDIATE_RETURN_ENABLED = "returnImmediately";

        static final String MAX_MESSAGE_COUNT = "maxMessages";

    }

    @JsonProperty(JsonFieldName.IMMEDIATE_RETURN_ENABLED)
    private final boolean immediateReturnEnabled;

    @JsonProperty(JsonFieldName.MAX_MESSAGE_COUNT)
    private final int maxMessageCount;

    public PubsubPullRequest(boolean immediateReturnEnabled, int maxMessageCount) {
        if (maxMessageCount <= 0) {
            String message = "was expecting a positive non-zero maxMessageCount, found " + maxMessageCount;
            throw new IllegalArgumentException(message);
        }
        this.immediateReturnEnabled = immediateReturnEnabled;
        this.maxMessageCount = maxMessageCount;
    }

    public boolean isImmediateReturnEnabled() {
        return immediateReturnEnabled;
    }

    public int getMaxMessageCount() {
        return maxMessageCount;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubPullRequest that = (PubsubPullRequest) object;
        return immediateReturnEnabled == that.immediateReturnEnabled &&
                maxMessageCount == that.maxMessageCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(immediateReturnEnabled, maxMessageCount);
    }

    @Override
    public String toString() {
        return "PubsubPullRequest{" +
                "immediateReturnEnabled=" + immediateReturnEnabled +
                ", maxMessageCount=" + maxMessageCount +
                '}';
    }

}
