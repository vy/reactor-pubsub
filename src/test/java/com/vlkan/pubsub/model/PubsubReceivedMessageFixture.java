package com.vlkan.pubsub.model;

import java.util.concurrent.atomic.AtomicInteger;

public enum PubsubReceivedMessageFixture {;

    private static final AtomicInteger MESSAGE_COUNTER_REF = new AtomicInteger(0);

    public static PubsubReceivedMessage createRandomReceivedMessage() {
        PubsubReceivedMessageEmbedding embedding = PubsubReceivedMessageEmbeddingFixture.createRandomReceivedMessageEmbedding();
        int messageIndex = MESSAGE_COUNTER_REF.getAndIncrement();
        String ackId = String.format("ackId-%04d", messageIndex);
        return new PubsubReceivedMessage(ackId, embedding);
    }

}
