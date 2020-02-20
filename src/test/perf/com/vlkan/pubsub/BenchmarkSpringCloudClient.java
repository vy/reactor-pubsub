package com.vlkan.pubsub;

import io.netty.channel.epoll.Epoll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.gcp.pubsub.reactive.PubSubReactiveFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.util.concurrent.ListenableFuture;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.net.URL;
import java.time.Duration;

public class BenchmarkSpringCloudClient implements BenchmarkClient {

    static { Epoll.ensureAvailability(); }

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkSpringCloudClient.class);

    private final String baseUrl;

    private final int concurrency;

    private final long maxMessageCount;

    private final long warmUpPeriodSeconds;

    private BenchmarkSpringCloudClient(
            String baseUrl,
            int concurrency,
            long maxMessageCount,
            long warmUpPeriodSeconds) {
        this.baseUrl = baseUrl;
        this.concurrency = concurrency;
        this.maxMessageCount = maxMessageCount;
        this.warmUpPeriodSeconds = warmUpPeriodSeconds;
    }

    @SpringBootApplication
    public static class Application {

        private final PubSubReactiveFactory reactiveFactory;

        public Application(PubSubReactiveFactory reactiveFactory) {
            this.reactiveFactory = reactiveFactory;
        }

        private void pullAndAck(
                String label,
                long maxMessageCount,
                @Nullable Duration timespan) {
            reactiveFactory
                    .poll(BenchmarkConstants.SUBSCRIPTION_NAME, 10_000L)
                    .retry()
                    .flatMap(acknowledgeablePubsubMessage -> {
                        ListenableFuture<Void> ackFuture = acknowledgeablePubsubMessage.ack();
                        return Mono
                                .create(sink -> ackFuture.addCallback(
                                        ignored -> sink.success(),
                                        sink::error))
                                .retry()
                                .thenReturn(1);
                    })
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

    }

    @Override
    public void run() throws Exception {

        // Create the Spring application.
        LOGGER.info("creating Spring application");
        URL typedBaseUrl = new URL(baseUrl);
        String host = typedBaseUrl.getHost();
        int port = typedBaseUrl.getPort();
        ConfigurableApplicationContext context =
                new SpringApplicationBuilder(Application.class)
                        .properties("spring.cloud.gcp.pubsub.emulator-host=" + host + ':' + port)
                        .properties("spring.cloud.gcp.pubsub.subscriber.parallel-pull-count=" + concurrency)
                        .run();

        // Executing pull-and-ack loops.
        Application app = context.getBean(Application.class);
        app.pullAndAck("warm-up", maxMessageCount, Duration.ofSeconds(warmUpPeriodSeconds));
        app.pullAndAck("benchmark", maxMessageCount, null);

        // Shutdown the Spring application.
        context.close();

    }

    public static void main(String[] args) throws Exception {
        BenchmarkClients.run(BenchmarkSpringCloudClient::new);
    }

}
