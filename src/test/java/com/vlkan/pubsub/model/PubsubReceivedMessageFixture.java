package com.vlkan.pubsub.model;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public enum PubsubReceivedMessageFixture {;

    private static final AtomicInteger MESSAGE_COUNTER_REF = new AtomicInteger(0);

    private static final Instant INIT_INSTANT = Instant.parse("2019-11-08T09:58:53Z");

    public static PubsubReceivedMessage createRandomReceivedMessage() {
        int messageIndex = MESSAGE_COUNTER_REF.getAndIncrement();
        Instant publishInstant = INIT_INSTANT.plus(Duration.ofSeconds(messageIndex));
        String id = String.format("id-%04d", messageIndex);
        byte[] payload = String
                .format("payload-%04d", messageIndex)
                .getBytes(StandardCharsets.UTF_8);
        String attributeKey = String.format("key-%04d", messageIndex);
        String attributeVal = String.format("val-%04d", messageIndex);
        Map<String, String> attributes = Collections.singletonMap(attributeKey, attributeVal);
        return new PubsubReceivedMessage(publishInstant, id, payload, attributes);
    }

}
