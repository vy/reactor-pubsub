package com.vlkan.pubsub;

@FunctionalInterface
interface BenchmarkServerFactory {

    BenchmarkServer create(
            String host,
            int port,
            int distinctResponseCount,
            int perPullMessageCount,
            int messagePayloadLength);

}
