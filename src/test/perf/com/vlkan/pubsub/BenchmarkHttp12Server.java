package com.vlkan.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchmarkHttp12Server implements BenchmarkServer {

    static { Epoll.ensureAvailability(); }

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkHttp12Server.class);

    private enum RequestRelativePath {;

        private static final String PULL =
                PubsubClient.createPullRequestRelativePath(
                        BenchmarkConstants.PROJECT_NAME,
                        BenchmarkConstants.SUBSCRIPTION_NAME);

        private static final String ACK =
                PubsubClient.createAckRequestRelativePath(
                        BenchmarkConstants.PROJECT_NAME,
                        BenchmarkConstants.SUBSCRIPTION_NAME);

    }

    private final String host;

    private final int port;

    private final int distinctResponseCount;

    private final int perPullMessageCount;

    private final int messagePayloadLength;

    private BenchmarkHttp12Server(
            String host,
            int port,
            int distinctResponseCount,
            int perPullMessageCount,
            int messagePayloadLength) {
        this.host = host;
        this.port = port;
        this.distinctResponseCount = distinctResponseCount;
        this.perPullMessageCount = perPullMessageCount;
        this.messagePayloadLength = messagePayloadLength;
    }

    @Override
    public void run() {

        LOGGER.info("building responses");
        List<PubsubPullResponse> pullResponses = createPullResponses();
        List<byte[]> jsonPullResponsePayloads = createPullResponsePayloads(pullResponses);

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
                                BenchmarkHttp12Server::handleAck))
                .host(host)
                .port(port)
                .bindUntilJavaShutdown(
                        Duration.ofHours(1),
                        ignored -> LOGGER.info("started server"));

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
                .range(0, perPullMessageCount)
                .mapToObj(messageIndex -> createReceivedMessage(pullResponseIndex, messageIndex))
                .collect(Collectors.toList());
    }

    private PubsubReceivedMessage createReceivedMessage(
            int pullResponseIndex,
            int messageIndex) {
        String ackId = BenchmarkPayloads.createAckId(pullResponseIndex, messageIndex);
        PubsubReceivedMessageEmbedding embedding =
                createReceivedMessageEmbedding(pullResponseIndex, messageIndex);
        return new PubsubReceivedMessage(ackId, embedding);
    }

    private PubsubReceivedMessageEmbedding createReceivedMessageEmbedding(
            int pullResponseIndex,
            int messageIndex) {
        Instant publishInstant = BenchmarkPayloads
                .START_INSTANT
                .plus(Duration.ofDays(pullResponseIndex))
                .plus(Duration.ofSeconds(messageIndex));
        String id = BenchmarkPayloads.createMessageId(pullResponseIndex, messageIndex);
        byte[] payload = BenchmarkPayloads.createRandomBytes(messagePayloadLength);
        Map<String, String> attributes = Collections.emptyMap();
        return new PubsubReceivedMessageEmbedding(publishInstant, id, payload, attributes);
    }

    private static List<byte[]> createPullResponsePayloads(
            List<PubsubPullResponse> pullResponses) {
        return pullResponses
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
                    int responsePayloadIndex = BenchmarkPayloads.RANDOM.nextInt(responsePayloads.size());
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

    public static void main(String[] args) throws Exception {
        BenchmarkServers.run(BenchmarkHttp12Server::new);
    }

}
