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

package com.vlkan.pubsub;

import java.time.Duration;
import java.util.Objects;

public class PubsubPullerConfig {

    public static final int DEFAULT_PULL_BUFFER_SIZE = 100;

    public static final Duration DEFAULT_PULL_PERIOD = Duration.ZERO;

    public static final int DEFAULT_PULL_CONCURRENCY = Runtime.getRuntime().availableProcessors();

    private final int pullBufferSize;

    private final Duration pullPeriod;

    private final int pullConcurrency;

    private final String projectName;

    private final String subscriptionName;

    private PubsubPullerConfig(Builder builder) {
        this.pullBufferSize = builder.pullBufferSize;
        this.pullPeriod = builder.pullPeriod;
        this.pullConcurrency = builder.pullConcurrency;
        this.projectName = builder.projectName;
        this.subscriptionName = builder.subscriptionName;
    }

    public int getPullBufferSize() {
        return pullBufferSize;
    }

    public Duration getPullPeriod() {
        return pullPeriod;
    }

    public int getPullConcurrency() {
        return pullConcurrency;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubPullerConfig that = (PubsubPullerConfig) object;
        return pullBufferSize == that.pullBufferSize &&
                pullConcurrency == that.pullConcurrency &&
                pullPeriod.equals(that.pullPeriod) &&
                projectName.equals(that.projectName) &&
                subscriptionName.equals(that.subscriptionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                pullBufferSize,
                pullConcurrency,
                pullPeriod,
                projectName,
                subscriptionName);
    }

    @Override
    public String toString() {
        return "PubsubPullerConfig{" +
                "projectName='" + projectName + '\'' +
                ", subscriptionName='" + subscriptionName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private int pullBufferSize = DEFAULT_PULL_BUFFER_SIZE;

        private Duration pullPeriod = DEFAULT_PULL_PERIOD;

        private int pullConcurrency = DEFAULT_PULL_CONCURRENCY;

        private String projectName;

        private String subscriptionName;

        private Builder() {}

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

        public Builder setPullConcurrency(int pullConcurrency) {
            if (pullConcurrency < 1) {
                throw new IllegalArgumentException(
                        "was expecting a non-zero positive pull concurrency");
            }
            this.pullConcurrency = pullConcurrency;
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

        public PubsubPullerConfig build() {
            Objects.requireNonNull(projectName, "projectName");
            Objects.requireNonNull(subscriptionName, "subscriptionName");
            return new PubsubPullerConfig(this);
        }

    }

}
