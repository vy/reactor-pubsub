package com.vlkan.pubsub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum BenchmarkClients {;

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkClients.class);

    private static final String DEFAULT_BASE_URL =
            "http://" + BenchmarkConstants.DEFAULT_SERVER_HOST + ':' + BenchmarkConstants.DEFAULT_SERVER_PORT;

    static void run(BenchmarkClientFactory clientFactory) throws Exception {

        // Read configuration.
        String baseUrl = BenchmarkProperties.getStringProperty("benchmark.baseUrl", DEFAULT_BASE_URL);
        int concurrency = BenchmarkProperties.getIntProperty("benchmark.concurrency", 2);
        long maxMessageCount  = BenchmarkProperties.getLongProperty("benchmark.maxMessageCount", 1_000_000L);
        int warmUpPeriodSeconds = BenchmarkProperties.getIntProperty("benchmark.warmUpPeriodSeconds", 30);

        // Log configuration.
        LOGGER.info("baseUrl = {}", baseUrl);
        LOGGER.info("concurrency = {}", concurrency);
        LOGGER.info("maxMessageCount = {}", maxMessageCount);
        LOGGER.info("warmUpPeriodSeconds = {}", warmUpPeriodSeconds);

        // Start the client.
        BenchmarkClient client = clientFactory.create(baseUrl, concurrency, maxMessageCount, warmUpPeriodSeconds);
        client.run();

    }

}
