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

import com.vlkan.pubsub.model.PubsubAckRequest;
import com.vlkan.pubsub.model.PubsubPullResponse;

import java.util.Objects;

public class PubsubSubscriberConsumption {

    private final PubsubPullResponse pullResponse;

    private final PubsubAckRequest ackRequest;

    PubsubSubscriberConsumption(PubsubPullResponse pullResponse, PubsubAckRequest ackRequest) {
        this.pullResponse = pullResponse;
        this.ackRequest = ackRequest;
    }

    public PubsubPullResponse getPullResponse() {
        return pullResponse;
    }

    public PubsubAckRequest getAckRequest() {
        return ackRequest;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        PubsubSubscriberConsumption that = (PubsubSubscriberConsumption) object;
        return pullResponse.equals(that.pullResponse) &&
                ackRequest.equals(that.ackRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pullResponse, ackRequest);
    }

}
