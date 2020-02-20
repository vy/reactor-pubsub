package com.vlkan.pubsub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum BenchmarkServers {;

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkServers.class);

    static void run(BenchmarkServerFactory serverFactory) throws Exception {

        // Read configuration.
        String host = BenchmarkProperties.getStringProperty("benchmark.host", BenchmarkConstants.DEFAULT_SERVER_HOST);
        int port = BenchmarkProperties.getIntProperty("benchmark.port", BenchmarkConstants.DEFAULT_SERVER_PORT);
        int distinctResponseCount = BenchmarkProperties.getIntProperty("benchmark.distinctResponseCount", 10);
        int perPullMessageCount = BenchmarkProperties.getIntProperty("benchmark.perPullMessageCount", 100);
        int messagePayloadLength = BenchmarkProperties.getIntProperty("benchmark.messagePayloadLength", 1024);

        // Log configuration.
        LOGGER.info("host = {}", host);
        LOGGER.info("port = {}", port);
        LOGGER.info("distinctResponseCount = {}", distinctResponseCount);
        LOGGER.info("perPullMessageCount = {}", perPullMessageCount);
        LOGGER.info("messagePayloadLength = {}", messagePayloadLength);

        // Start the server.
        BenchmarkServer server = serverFactory.create(host, port, distinctResponseCount, perPullMessageCount, messagePayloadLength);
        server.run();

    }

}
