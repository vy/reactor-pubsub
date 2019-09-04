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

import java.util.Objects;

public class PubsubPublisherConfig {

    private final String projectName;

    private final String topicName;

    private PubsubPublisherConfig(Builder builder) {
        this.projectName = builder.projectName;
        this.topicName = builder.topicName;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubPublisherConfig that = (PubsubPublisherConfig) object;
        return projectName.equals(that.projectName) &&
                topicName.equals(that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectName, topicName);
    }

    @Override
    public String toString() {
        return "PubsubPublisherConfig{" +
                "projectName='" + projectName + '\'' +
                ", topicName='" + topicName + '\'' +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String projectName;

        private String topicName;

        private Builder() {}

        public Builder setProjectName(String projectName) {
            this.projectName = Objects.requireNonNull(projectName, "projectName");
            return this;
        }

        public Builder setTopicName(String topicName) {
            this.topicName = Objects.requireNonNull(topicName, "topicName");
            return this;
        }

        public PubsubPublisherConfig build() {
            Objects.requireNonNull(projectName, "projectName");
            Objects.requireNonNull(topicName, "topicName");
            return new PubsubPublisherConfig(this);
        }

    }

}
