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

package com.vlkan.pubsub.model;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public enum PubsubReceivedMessageEmbeddingFixture {;

    private static final AtomicInteger MESSAGE_COUNTER_REF = new AtomicInteger(0);

    private static final Instant INIT_INSTANT = Instant.parse("2019-11-08T09:58:53Z");

    public static PubsubReceivedMessageEmbedding createRandomReceivedMessageEmbedding() {
        int messageIndex = MESSAGE_COUNTER_REF.getAndIncrement();
        Instant publishInstant = INIT_INSTANT.plus(Duration.ofSeconds(messageIndex));
        String id = String.format("id-%04d", messageIndex);
        byte[] payload = String
                .format("payload-%04d", messageIndex)
                .getBytes(StandardCharsets.UTF_8);
        String attributeKey = String.format("key-%04d", messageIndex);
        String attributeVal = String.format("val-%04d", messageIndex);
        Map<String, String> attributes = Collections.singletonMap(attributeKey, attributeVal);
        return new PubsubReceivedMessageEmbedding(publishInstant, id, payload, attributes);
    }

}
