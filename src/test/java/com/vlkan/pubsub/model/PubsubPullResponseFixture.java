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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public enum PubsubPullResponseFixture {;

    public static PubsubPullResponse createRandomPullResponse(int messageCount) {
        List<PubsubReceivedMessage> receivedMessages = IntStream
                .range(0, messageCount)
                .mapToObj(ignored -> PubsubReceivedMessageFixture.createRandomReceivedMessage())
                .collect(Collectors.toList());
        return new PubsubPullResponse(receivedMessages);
    }

}
