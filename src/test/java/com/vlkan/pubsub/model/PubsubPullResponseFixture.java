package com.vlkan.pubsub.model;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public enum PubsubPullResponseFixture {;

    public static PubsubPullResponse createRandomPullResponse(int messageCount) {
        List<PubsubReceivedAckableMessage> receivedAckableMessages = IntStream
                .range(0, messageCount)
                .mapToObj(ignored -> PubsubReceivedAckableMessageFixture.createRandomReceivedAckableMessage())
                .collect(Collectors.toList());
        return new PubsubPullResponse(receivedAckableMessages);
    }

}
