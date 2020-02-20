package com.vlkan.pubsub;

@FunctionalInterface
interface BenchmarkClientFactory {

    BenchmarkClient create(
            String baseUrl,
            int concurrency,
            long maxMessageCount,
            long warmUpPeriodSeconds);

}
