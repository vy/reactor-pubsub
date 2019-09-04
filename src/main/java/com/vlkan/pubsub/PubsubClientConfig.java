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

package com.vlkan.pubsub;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Objects;

public class PubsubClientConfig {

    public static final String DEFAULT_BASE_URL = "https://pubsub.googleapis.com:443";

    public static final Duration DEFAULT_PULL_TIMEOUT= Duration.ofSeconds(30);

    public static final Duration DEFAULT_PUBLISH_TIMEOUT = Duration.ofSeconds(30);

    public static final Duration DEFAULT_ACK_TIMEOUT = Duration.ofSeconds(10);

    public static final String DEFAULT_USER_AGENT = "reactor-pubsub";

    public static final String DEFAULT_METER_NAME = "pubsub.client";

    public static final PubsubClientConfig DEFAULT = builder().build();

    private final String baseUrl;

    private final Duration pullTimeout;

    private final Duration publishTimeout;

    private final Duration ackTimeout;

    @Nullable
    private final String userAgent;

    private final String meterName;

    private PubsubClientConfig(Builder builder) {
        this.baseUrl = builder.baseUrl;
        this.pullTimeout = builder.pullTimeout;
        this.publishTimeout = builder.publishTimeout;
        this.ackTimeout = builder.ackTimeout;
        this.userAgent = builder.userAgent;
        this.meterName = builder.meterName;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public Duration getPullTimeout() {
        return pullTimeout;
    }

    public Duration getPublishTimeout() {
        return publishTimeout;
    }

    public Duration getAckTimeout() {
        return ackTimeout;
    }

    @Nullable
    public String getUserAgent() {
        return userAgent;
    }

    public String getMeterName() {
        return meterName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String baseUrl = DEFAULT_BASE_URL;

        private Duration pullTimeout = DEFAULT_PULL_TIMEOUT;

        private Duration publishTimeout = DEFAULT_PUBLISH_TIMEOUT;

        private Duration ackTimeout = DEFAULT_ACK_TIMEOUT;

        @Nullable
        private String userAgent = DEFAULT_USER_AGENT;

        private String meterName = DEFAULT_METER_NAME;

        private Builder() {}

        public Builder setBaseUrl(String baseUrl) {
            try {
                new URL(baseUrl);
            } catch (MalformedURLException error) {
                String message = String.format("malformed URL (baseUrl=%s)", baseUrl);
                throw new RuntimeException(message, error);
            }
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder setPullTimeout(Duration pullTimeout) {
            this.pullTimeout = Objects.requireNonNull(pullTimeout, "pullTimeout");
            return this;
        }

        public Builder setPublishTimeout(Duration publishTimeout) {
            this.publishTimeout = Objects.requireNonNull(publishTimeout, "publishTimeout");
            return this;
        }

        public Builder setAckTimeout(Duration ackTimeout) {
            this.ackTimeout = Objects.requireNonNull(ackTimeout, "ackTimeout");
            return this;
        }

        public Builder setUserAgent(@Nullable String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        public Builder setMeterName(String meterName) {
            this.meterName = Objects.requireNonNull(meterName, "meterName");
            return this;
        }

        public PubsubClientConfig build() {
            return new PubsubClientConfig(this);
        }

    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubClientConfig that = (PubsubClientConfig) object;
        return baseUrl.equals(that.baseUrl) &&
                pullTimeout.equals(that.pullTimeout) &&
                publishTimeout.equals(that.publishTimeout) &&
                ackTimeout.equals(that.ackTimeout) &&
                Objects.equals(userAgent, that.userAgent) &&
                meterName.equals(that.meterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                baseUrl,
                pullTimeout,
                publishTimeout,
                ackTimeout,
                userAgent,
                meterName);
    }

}
