package com.vlkan.pubsub;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.epoll.Epoll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BenchmarkGrpcServer implements BenchmarkServer {

    static { Epoll.ensureAvailability(); }

    private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkGrpcServer.class);

    private final int port;

    private final int distinctResponseCount;

    private final int perPullMessageCount;

    private final int messagePayloadLength;

    private BenchmarkGrpcServer(
            String host,
            int port,
            int distinctResponseCount,
            int perPullMessageCount,
            int messagePayloadLength) {
        this.port = port;
        this.distinctResponseCount = distinctResponseCount;
        this.perPullMessageCount = perPullMessageCount;
        this.messagePayloadLength = messagePayloadLength;
    }

    @Override
    public void run() throws Exception {

        LOGGER.info("building responses");
        List<PullResponse> pullResponses = createPullResponses();

        LOGGER.info("starting server");
        SubscriberImpl subscriber = new SubscriberImpl(pullResponses);
        Server server = ServerBuilder
                .forPort(port)
                .addService(subscriber)
                .build()
                .start();
        LOGGER.info("started server");

        try {
            server.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException ignored) {
            // Do nothing.
        }

    }

    private List<PullResponse> createPullResponses() {
        return IntStream
                .range(0, distinctResponseCount)
                .mapToObj(this::createReceivedMessages)
                .map(receivedMessages -> PullResponse
                        .newBuilder()
                        .addAllReceivedMessages(receivedMessages)
                        .build())
                .collect(Collectors.toList());
    }

    private List<ReceivedMessage> createReceivedMessages(
            int pullResponseIndex) {
        return IntStream
                .range(0, perPullMessageCount)
                .mapToObj(messageIndex -> createReceivedMessage(pullResponseIndex, messageIndex))
                .collect(Collectors.toList());
    }

    private ReceivedMessage createReceivedMessage(
            int pullResponseIndex,
            int messageIndex) {
        String ackId = BenchmarkPayloads.createAckId(pullResponseIndex, messageIndex);
        PubsubMessage message = createMessage(pullResponseIndex, messageIndex);
        return ReceivedMessage
                .newBuilder()
                .setAckId(ackId)
                .setMessage(message)
                .build();
    }

    private PubsubMessage createMessage(
            int pullResponseIndex,
            int messageIndex) {
        Instant publishInstant = BenchmarkPayloads
                .START_INSTANT
                .plus(Duration.ofDays(pullResponseIndex))
                .plus(Duration.ofSeconds(messageIndex));
        Timestamp publishInstantTimestamp = Timestamp
                .newBuilder()
                .setSeconds(publishInstant.getEpochSecond())
                .setNanos(publishInstant.getNano())
                .build();
        String id = BenchmarkPayloads.createMessageId(pullResponseIndex, messageIndex);
        byte[] payload = BenchmarkPayloads.createRandomBytes(messagePayloadLength);
        ByteString payloadByteString = ByteString.copyFrom(payload);
        return PubsubMessage
                .newBuilder()
                .setMessageId(id)
                .setPublishTime(publishInstantTimestamp)
                .setData(payloadByteString)
                .build();
    }

    private static final class SubscriberImpl extends SubscriberGrpc.SubscriberImplBase {

        private final List<PullResponse> pullResponses;

        private SubscriberImpl(List<PullResponse> pullResponses) {
            this.pullResponses = pullResponses;
        }

        @Override
        public void pull(PullRequest ignored, StreamObserver<PullResponse> responseObserver) {
            int pullResponseIndex = BenchmarkPayloads.RANDOM.nextInt(pullResponses.size());
            PullResponse pullResponse = pullResponses.get(pullResponseIndex);
            responseObserver.onNext(pullResponse);
            responseObserver.onCompleted();
        }

        @Override
        public void acknowledge(AcknowledgeRequest ignored, StreamObserver<Empty> responseObserver) {
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        }

    }

    public static void main(String[] args) throws Exception {
        BenchmarkServers.run(BenchmarkGrpcServer::new);
    }

}
