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

import java.util.Objects;

public class PubsubAckerConfig {

    private final String projectName;

    private final String subscriptionName;

    private PubsubAckerConfig(Builder builder) {
        this.projectName = builder.projectName;
        this.subscriptionName = builder.subscriptionName;
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
        PubsubAckerConfig that = (PubsubAckerConfig) object;
        return projectName.equals(that.projectName) &&
                subscriptionName.equals(that.subscriptionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectName, subscriptionName);
    }

    @Override
    public String toString() {
        return "PubsubAckerConfig{" +
                "projectName='" + projectName + '\'' +
                ", subscriptionName='" + subscriptionName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String projectName;

        private String subscriptionName;

        private Builder() {}

        public Builder setProjectName(String projectName) {
            this.projectName = Objects.requireNonNull(projectName, "projectName");
            return this;
        }

        public Builder setSubscriptionName(String subscriptionName) {
            this.subscriptionName = Objects.requireNonNull(subscriptionName, "subscriptionName");
            return this;
        }

        public PubsubAckerConfig build() {
            Objects.requireNonNull(projectName, "projectName");
            Objects.requireNonNull(subscriptionName, "subscriptionName");
            return new PubsubAckerConfig(this);
        }

    }

}
