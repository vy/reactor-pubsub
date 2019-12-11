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
