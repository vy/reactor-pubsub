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

import java.time.Duration;
import java.util.Objects;

public class PubsubSubscriberConfig {

    public static final int DEFAULT_PULL_CONCURRENCY = 4;

    public static final int DEFAULT_PULL_BUFFER_SIZE = 100;

    public static final Duration DEFAULT_PULL_PERIOD = Duration.ofSeconds(30);

    public static final String DEFAULT_RATE_LIMITER_SPEC = "1/1m:, 1/30s:1/1m, 1/1s:2/1m, :1/3m";

    public static final String DEFAULT_METER_NAME = "pubsub.subscriber";

    private int pullConcurrency;

    private int pullBufferSize;

    private Duration pullPeriod;

    private String rateLimiterSpec;

    private String meterName;

    private String projectName;

    private String subscriptionName;

    private PubsubSubscriberConfig(Builder builder) {
        this.pullConcurrency = builder.pullConcurrency;
        this.pullBufferSize = builder.pullBufferSize;
        this.pullPeriod = builder.pullPeriod;
        this.rateLimiterSpec = builder.rateLimiterSpec;
        this.meterName = builder.meterName;
        this.projectName = builder.projectName;
        this.subscriptionName = builder.projectName;
    }

    public int getPullConcurrency() {
        return pullConcurrency;
    }

    public int getPullBufferSize() {
        return pullBufferSize;
    }

    public Duration getPullPeriod() {
        return pullPeriod;
    }

    public String getRateLimiterSpec() {
        return rateLimiterSpec;
    }

    public String getMeterName() {
        return meterName;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private int pullConcurrency = DEFAULT_PULL_CONCURRENCY;

        private int pullBufferSize = DEFAULT_PULL_BUFFER_SIZE;

        private Duration pullPeriod = DEFAULT_PULL_PERIOD;

        private String rateLimiterSpec = DEFAULT_RATE_LIMITER_SPEC;

        private String meterName = DEFAULT_METER_NAME;

        private String projectName;

        private String subscriptionName;

        private Builder() {}

        public Builder setPullConcurrency(int pullConcurrency) {
            if (pullConcurrency < 1) {
                throw new IllegalArgumentException(
                        "was expecting a non-zero positive pull concurrency");
            }
            this.pullConcurrency = pullConcurrency;
            return this;
        }

        public Builder setPullBufferSize(int pullBufferSize) {
            if (pullBufferSize < 1) {
                throw new IllegalArgumentException(
                        "was expecting a non-zero positive pull buffer size");
            }
            this.pullBufferSize = pullBufferSize;
            return this;
        }

        public Builder setPullPeriod(Duration pullPeriod) {
            this.pullPeriod = Objects.requireNonNull(pullPeriod, "pullPeriod");
            return this;
        }

        public Builder setRateLimiterSpec(String rateLimiterSpec) {
            this.rateLimiterSpec = Objects.requireNonNull(rateLimiterSpec, "rateLimiterSpec");
            return this;
        }

        public Builder setMeterName(String meterName) {
            this.meterName = Objects.requireNonNull(meterName, "meterName");
            return this;
        }

        public Builder setProjectName(String projectName) {
            this.projectName = Objects.requireNonNull(projectName, "projectName");
            return this;
        }

        public Builder setSubscriptionName(String subscriptionName) {
            this.subscriptionName = Objects.requireNonNull(subscriptionName, "subscriptionName");
            return this;
        }

        public PubsubSubscriberConfig build() {
            Objects.requireNonNull(projectName, "projectName");
            Objects.requireNonNull(subscriptionName, "subscriptionName");
            return new PubsubSubscriberConfig(this);
        }

    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubSubscriberConfig that = (PubsubSubscriberConfig) object;
        return pullConcurrency == that.pullConcurrency &&
                pullBufferSize == that.pullBufferSize &&
                pullPeriod.equals(that.pullPeriod) &&
                rateLimiterSpec.equals(that.rateLimiterSpec) &&
                meterName.equals(that.meterName) &&
                projectName.equals(that.projectName) &&
                subscriptionName.equals(that.subscriptionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                pullConcurrency,
                pullBufferSize,
                pullPeriod,
                rateLimiterSpec,
                meterName,
                projectName,
                subscriptionName);
    }

    @Override
    public String toString() {
        return "PubsubSubscriberConfig{" +
                "projectName='" + projectName + '\'' +
                ", subscriptionName='" + subscriptionName + '\'' +
                '}';
    }

}
