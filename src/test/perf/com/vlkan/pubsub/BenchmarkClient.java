package com.vlkan.pubsub;

import io.netty.channel.epoll.Epoll;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.Callable;

public class BenchmarkClient implements Callable<Integer> {

    static { Epoll.ensureAvailability(); }

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkClient.class);

    private static final String DEFAULT_BASE_URL =
            "http://" + BenchmarkConstants.DEFAULT_SERVER_HOST + ':' + BenchmarkConstants.DEFAULT_SERVER_PORT;

    private final String baseUrl;

    private final int concurrency;

    private final int pullCount;

    private final long warmUpPeriodSeconds;

    private BenchmarkClient(
            String baseUrl,
            int concurrency,
            int pullCount,
            long warmUpPeriodSeconds) {
        this.baseUrl = baseUrl;
        this.concurrency = concurrency;
        this.pullCount = pullCount;
        this.warmUpPeriodSeconds = warmUpPeriodSeconds;
        LOGGER.info("baseUrl = {}", baseUrl);
        LOGGER.info("concurrency = {}", concurrency);
        LOGGER.info("pullCount = {}", pullCount);
        LOGGER.info("warmUpPeriodSeconds = {}", warmUpPeriodSeconds);
    }

    @Override
    public Integer call() {

        // Mock the access token cache.
        PubsubAccessTokenCache accessTokenCache = Mockito.mock(PubsubAccessTokenCache.class);
        Mockito.when(accessTokenCache.getAccessToken()).thenReturn("benchmark-token");

        // Create client.
        PubsubClientConfig clientConfig = PubsubClientConfig
                .builder()
                .setBaseUrl(baseUrl)
                .build();
        PubsubClient client = PubsubClient
                .builder()
                .setConfig(clientConfig)
                .setAccessTokenCache(accessTokenCache)
                .build();

        // Create puller.
        PubsubPullerConfig pullerConfig = PubsubPullerConfig
                .builder()
                .setProjectName(BenchmarkConstants.PROJECT_NAME)
                .setSubscriptionName(BenchmarkConstants.SUBSCRIPTION_NAME)
                .setPullConcurrency(concurrency)
                .build();
        PubsubPuller puller = PubsubPuller
                .builder()
                .setClient(client)
                .setConfig(pullerConfig)
                .build();

        // Create acker.
        PubsubAckerConfig ackerConfig = PubsubAckerConfig
                .builder()
                .setProjectName(BenchmarkConstants.PROJECT_NAME)
                .setSubscriptionName(BenchmarkConstants.SUBSCRIPTION_NAME)
                .build();
        PubsubAcker acker = PubsubAcker
                .builder()
                .setClient(client)
                .setConfig(ackerConfig)
                .build();

        // Pull-and-ack loop.
        pullAndAck(puller, acker, "warm-up", Duration.ofSeconds(warmUpPeriodSeconds));
        pullAndAck(puller, acker, "benchmark", null);

        // Exit with success.
        return 0;

    }

    private void pullAndAck(
            PubsubPuller puller,
            PubsubAcker acker,
            String label,
            @Nullable Duration timespan) {
        puller
                .pullAll()
                .retry()
                .flatMap(pullResponse -> acker
                        .ackPullResponse(pullResponse)
                        .retry()
                        .thenReturn(pullResponse.getReceivedMessages().size()))
                .transform(receivedMessageCounts -> timespan != null
                        ? receivedMessageCounts.take(timespan)
                        : receivedMessageCounts.take(pullCount))
                .reduce(0L, Long::sum)
                .elapsed()
                .doOnNext(elapsedMillisAndMessageCount -> {
                    long elapsedMillis = elapsedMillisAndMessageCount.getT1();
                    long messageCount = elapsedMillisAndMessageCount.getT2();
                    LOGGER.info(
                            "[{}] pulled and ack'ed {} messages in {} ms",
                            label, messageCount, elapsedMillis);
                })
                .doFirst(() -> LOGGER.info("[{}] started", label))
                .block();
    }

    public static void main(String[] args) {
        String baseUrl = BenchmarkHelpers.getStringProperty("benchmark.baseUrl", DEFAULT_BASE_URL);
        int concurrency = BenchmarkHelpers.getIntProperty("benchmark.concurrency", 2);
        int pullCount  = BenchmarkHelpers.getIntProperty("benchmark.pullCount", 1000);
        int warmUpPeriodSeconds = BenchmarkHelpers.getIntProperty("benchmark.warmUpPeriodSeconds", 30);
        BenchmarkClient client = new BenchmarkClient(baseUrl, concurrency, pullCount, warmUpPeriodSeconds);
        int exitCode = client.call();
        System.exit(exitCode);
    }

}
