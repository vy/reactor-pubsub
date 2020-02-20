package com.vlkan.pubsub;

import io.netty.channel.epoll.Epoll;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;

public class BenchmarkHttp12Client implements BenchmarkClient {

    static { Epoll.ensureAvailability(); }

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkHttp12Client.class);

    private final String baseUrl;

    private final int concurrency;

    private final long maxMessageCount;

    private final long warmUpPeriodSeconds;

    private BenchmarkHttp12Client(
            String baseUrl,
            int concurrency,
            long maxMessageCount,
            long warmUpPeriodSeconds) {
        this.baseUrl = baseUrl;
        this.concurrency = concurrency;
        this.maxMessageCount = maxMessageCount;
        this.warmUpPeriodSeconds = warmUpPeriodSeconds;
    }

    @Override
    public void run() {

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
                .transform(receivedMessageCounts -> {
                    if (timespan != null) {
                        return receivedMessageCounts
                                .take(timespan)
                                .reduce(0L, Long::sum);
                    } else {
                        return receivedMessageCounts
                                .scan(0L, Long::sum)
                                .takeWhile(messageCount -> messageCount < maxMessageCount)
                                .last();
                    }
                })
                .singleOrEmpty()
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

    public static void main(String[] args) throws Exception {
        BenchmarkClients.run(BenchmarkHttp12Client::new);
    }

}
