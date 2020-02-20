package com.vlkan.pubsub;

import java.time.Instant;
import java.util.Random;

enum BenchmarkPayloads {;

    static final Random RANDOM = new Random(0);

    static final Instant START_INSTANT = Instant.parse("2019-12-13T00:00:00Z");

    static byte[] createRandomBytes(int payloadLength) {
        byte[] payload = new byte[payloadLength];
        for (int i = 0; i < payloadLength; i++) {
            payload[i] = (byte) BenchmarkPayloads.RANDOM.nextInt();
        }
        return payload;
    }

    static String createAckId(int pullResponseIndex, int messageIndex) {
        return String.format("ackId-%04d-%04d", pullResponseIndex, messageIndex);
    }

    static String createMessageId(int pullResponseIndex, int messageIndex) {
        return String.format("id-%04d-%04d", pullResponseIndex, messageIndex);
    }

}
