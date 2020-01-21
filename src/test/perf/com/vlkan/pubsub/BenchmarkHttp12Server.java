package com.vlkan.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vlkan.pubsub.model.PubsubPublishResponse;
import com.vlkan.pubsub.model.PubsubPullResponse;
import com.vlkan.pubsub.model.PubsubReceivedMessage;
import com.vlkan.pubsub.model.PubsubReceivedMessageEmbedding;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchmarkHttp12Server implements Callable<Integer> {

    static { Epoll.ensureAvailability(); }

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkHttp12Server.class);

    private static final Random RANDOM = new Random(0);

    private static final Instant START_INSTANT = Instant.parse("2019-12-13T00:00:00Z");

    private enum RequestRelativePath {;

        private static final String PULL =
                PubsubClient.createPullRequestRelativePath(
                        BenchmarkConstants.PROJECT_NAME,
                        BenchmarkConstants.SUBSCRIPTION_NAME);

        private static final String ACK =
                PubsubClient.createAckRequestRelativePath(
                        BenchmarkConstants.PROJECT_NAME,
                        BenchmarkConstants.SUBSCRIPTION_NAME);

        private static final String PUBLISH =
                PubsubClient.createPublishRequestRelativePath(
                        BenchmarkConstants.PROJECT_NAME,
                        BenchmarkConstants.TOPIC_NAME);

    }

    private final String host;

    private final int port;

    private final int distinctResponseCount;

    private final int messageCount;

    private final int payloadLength;

    private BenchmarkHttp12Server(
            String host,
            int port,
            int distinctResponseCount,
            int messageCount,
            int payloadLength) {
        this.host = host;
        this.port = port;
        this.distinctResponseCount = distinctResponseCount;
        this.messageCount = messageCount;
        this.payloadLength = payloadLength;
        LOGGER.info("host = {}", host);
        LOGGER.info("port = {}", port);
        LOGGER.info("distinctResponseCount = {}", distinctResponseCount);
        LOGGER.info("messageCount = {}", messageCount);
        LOGGER.info("payloadLength = {}", payloadLength);
    }

    @Override
    public Integer call() {

        LOGGER.info("building responses");
        List<PubsubPullResponse> pullResponses = createPullResponses();
        List<byte[]> jsonPullResponsePayloads = createPullResponsePayloads(pullResponses);
        List<PubsubPublishResponse> publishResponses = createPublishResponses();
        List<byte[]> jsonPublishResponsePayloads = createPublishResponsePayloads(publishResponses);

        LOGGER.info("starting server");
        HttpServer
                .create()
                .route(routes -> routes
                        .post(
                                RequestRelativePath.PULL,
                                (request, response) -> handlePull(
                                        jsonPullResponsePayloads,
                                        request,
                                        response))
                        .post(
                                RequestRelativePath.ACK,
                                BenchmarkHttp12Server::handleAck)
                        .post(
                                RequestRelativePath.PUBLISH,
                                (request, response) -> handlePublish(
                                        jsonPublishResponsePayloads,
                                        request,
                                        response)))
                .host(host)
                .port(port)
                .bindUntilJavaShutdown(
                        Duration.ofHours(1),
                        ignored -> LOGGER.info("started server"));

        return 0;

    }

    private List<PubsubPullResponse> createPullResponses() {
        return IntStream
                .range(0, distinctResponseCount)
                .mapToObj(this::createReceivedMessages)
                .map(PubsubPullResponse::new)
                .collect(Collectors.toList());
    }

    private List<PubsubReceivedMessage> createReceivedMessages(
            int pullResponseIndex) {
        return IntStream
                .range(0, messageCount)
                .mapToObj(messageIndex -> createReceivedMessage(pullResponseIndex, messageIndex))
                .collect(Collectors.toList());
    }

    private PubsubReceivedMessage createReceivedMessage(
            int pullResponseIndex,
            int messageIndex) {
        String ackId = String.format("ackId-%04d-%04d", pullResponseIndex, messageIndex);
        PubsubReceivedMessageEmbedding embedding =
                createReceivedMessageEmbedding(pullResponseIndex, messageIndex);
        return new PubsubReceivedMessage(ackId, embedding);
    }

    private PubsubReceivedMessageEmbedding createReceivedMessageEmbedding(
            int pullResponseIndex,
            int messageIndex) {
        Instant publishInstant = START_INSTANT
                .plus(Duration.ofDays(pullResponseIndex))
                .plus(Duration.ofSeconds(messageIndex));
        String id = String.format("id-%04d-%04d", pullResponseIndex, messageIndex);
        byte[] payload = createMessagePayload();
        Map<String, String> attributes = Collections.emptyMap();
        return new PubsubReceivedMessageEmbedding(publishInstant, id, payload, attributes);
    }

    private byte[] createMessagePayload() {
        byte[] payload = new byte[payloadLength];
        for (int i = 0; i < payloadLength; i++) {
            payload[i] = (byte) RANDOM.nextInt();
        }
        return payload;
    }

    private static List<byte[]> createPullResponsePayloads(
            List<PubsubPullResponse> pullResponses) {
        return pullResponses
                .stream()
                .map(BenchmarkHttp12Server::jsonSerialize)
                .collect(Collectors.toList());
    }

    private List<PubsubPublishResponse> createPublishResponses() {
        return IntStream
                .range(0, distinctResponseCount)
                .mapToObj(this::createPublishResponse)
                .collect(Collectors.toList());
    }

    private PubsubPublishResponse createPublishResponse(int publishResponseIndex) {
        List<String> messageIds = IntStream
                .range(0, messageCount)
                .mapToObj(messageId -> String.format(
                        "messageId-%04d-%04d",
                        publishResponseIndex, messageId))
                .collect(Collectors.toList());
        return new PubsubPublishResponse(messageIds);
    }

    private static List<byte[]> createPublishResponsePayloads(
            List<PubsubPublishResponse> publishResponses) {
        return publishResponses
                .stream()
                .map(BenchmarkHttp12Server::jsonSerialize)
                .collect(Collectors.toList());
    }

    private static byte[] jsonSerialize(Object instance) {
        ObjectMapper objectMapper = PubsubClient.getDefaultObjectMapper();
        try {
            return objectMapper.writeValueAsBytes(instance);
        } catch (JsonProcessingException error) {
            throw new RuntimeException(error);
        }
    }

    private static Publisher<Void> handlePull(
            List<byte[]> responsePayloads,
            HttpServerRequest request,
            HttpServerResponse response) {
        return request
                .receive()
                .then(Mono.defer(() -> {

                    // Determine the response.
                    int responsePayloadIndex = RANDOM.nextInt(responsePayloads.size());
                    ByteBuf responsePayload =
                            Unpooled.wrappedBuffer(
                                    responsePayloads.get(responsePayloadIndex));

                    // Feed the content.
                    return response
                            .header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                            .send(Mono.just(responsePayload))
                            .then();

                }));
    }

    private static Publisher<Void> handleAck(
            HttpServerRequest request,
            HttpServerResponse response) {
        return request
                .receive()
                .then(Mono.defer(() -> response
                        .header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                        // Here, replacing send(Mono) with send() causes
                        // "status and headers are already sent" errors.
                        .send(Mono.empty())
                        .then()));
    }

    private static Publisher<Void> handlePublish(
            List<byte[]> responsePayloads,
            HttpServerRequest request,
            HttpServerResponse response) {
        return request
                .receive()
                .then(Mono.defer(() -> {

                    // Determine the response.
                    int responsePayloadIndex = RANDOM.nextInt(responsePayloads.size());
                    ByteBuf responsePayload =
                            Unpooled.wrappedBuffer(
                                    responsePayloads.get(responsePayloadIndex));

                    // Feed the response.
                    return response
                            .header(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                            .send(Mono.just(responsePayload))
                            .then();

                }));
    }

    public static void main(String[] args) {
        String host = BenchmarkHelpers.getStringProperty("benchmark.host", BenchmarkConstants.DEFAULT_SERVER_HOST);
        int port = BenchmarkHelpers.getIntProperty("benchmark.port", BenchmarkConstants.DEFAULT_SERVER_PORT);
        int distinctResponseCount = BenchmarkHelpers.getIntProperty("benchmark.distinctResponseCount", 10);
        int messageCount = BenchmarkHelpers.getIntProperty("benchmark.messageCount", 100);
        int payloadLength = BenchmarkHelpers.getIntProperty("benchmark.payloadLength", 1024);
        BenchmarkHttp12Server server = new BenchmarkHttp12Server(host, port, distinctResponseCount, messageCount, payloadLength);
        int exitCode = server.call();
        System.exit(exitCode);
    }

}
