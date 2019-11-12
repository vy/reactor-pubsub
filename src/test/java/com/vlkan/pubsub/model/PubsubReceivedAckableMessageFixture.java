package com.vlkan.pubsub.model;

import java.util.concurrent.atomic.AtomicInteger;

public enum PubsubReceivedAckableMessageFixture {;

    private static final AtomicInteger MESSAGE_COUNTER_REF = new AtomicInteger(0);

    public static PubsubReceivedAckableMessage createRandomReceivedAckableMessage() {
        PubsubReceivedMessage receivedMessage = PubsubReceivedMessageFixture.createRandomReceivedMessage();
        int messageIndex = MESSAGE_COUNTER_REF.getAndIncrement();
        String ackId = String.format("ackId-0%4d", messageIndex);
        return new PubsubReceivedAckableMessage(ackId, receivedMessage);
    }

}
