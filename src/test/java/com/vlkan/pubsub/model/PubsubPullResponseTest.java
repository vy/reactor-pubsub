package com.vlkan.pubsub.model;

import com.vlkan.pubsub.MapHelpers;
import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PubsubPullResponseTest {

    @Test
    public void test_deserialization_with_empty_payload() {
        PubsubPullResponse actualResponse =
                JacksonHelpers.readValue("{}", PubsubPullResponse.class);
        Assertions.assertThat(actualResponse.getReceivedAckableMessages()).isEmpty();
    }

    @Test
    public void test_deserialization_with_null_receivedMessages() {
        PubsubPullResponse actualResponse =
                JacksonHelpers.readValue("{\"receivedMessages\": null}", PubsubPullResponse.class);
        Assertions.assertThat(actualResponse.getReceivedAckableMessages()).isEmpty();
    }

    @Test
    public void test_deserialization_with_empty_receivedMessages() {
        PubsubPullResponse actualResponse =
                JacksonHelpers.readValue("{\"receivedMessages\": []}", PubsubPullResponse.class);
        Assertions.assertThat(actualResponse.getReceivedAckableMessages()).isEmpty();
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant1 = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload1 = {1, 2, 3};
        Map<String, Object> expectedReceivedMessage1Map = MapHelpers.createMap(
                "publishTime", expectedInstant1.toString(),
                "messageId", "messageId1",
                "data", Base64.getEncoder().encodeToString(expectedPayload1));
        Map<String, Object> expectedReceivedAckableMessage1Map = MapHelpers.createMap(
                "ackId", "ackId1",
                "message", expectedReceivedMessage1Map);
        Instant expectedInstant2 = expectedInstant1.plus(Duration.ofMinutes(1));
        byte[] expectedPayload2 = {4, 5, 6};
        Map<String, Object> expectedReceivedMessage2Map = MapHelpers.createMap(
                "publishTime", expectedInstant2.toString(),
                "messageId", "messageId2",
                "data", Base64.getEncoder().encodeToString(expectedPayload2));
        Map<String, Object> expectedReceivedAckableMessage2Map = MapHelpers.createMap(
                "ackId", "ackId2",
                "message", expectedReceivedMessage2Map);
        List<Map<String, Object>> expectedReceivedAckableMessagesMap = Arrays.asList(
                expectedReceivedAckableMessage1Map,
                expectedReceivedAckableMessage2Map);
        Map<String, Object> expectedResponseMap = Collections.singletonMap(
                "receivedMessages", expectedReceivedAckableMessagesMap);
        String responseJson = JacksonHelpers.writeValueAsString(expectedResponseMap);

        // Deserialize Pub/Sub pull response from the JSON.
        PubsubPullResponse actualResponse =
                JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);

        // Build the expected response model.
        PubsubPullResponse expectedResponse = new PubsubPullResponse(Arrays.asList(
                new PubsubReceivedAckableMessage(
                        "ackId1",
                        new PubsubReceivedMessage(
                                expectedInstant1, "messageId1", expectedPayload1)),
                new PubsubReceivedAckableMessage(
                        "ackId2",
                        new PubsubReceivedMessage(
                                expectedInstant2, "messageId2", expectedPayload2))));

        // Compare the content.
        Assertions.assertThat(actualResponse).isEqualTo(expectedResponse);

    }

    @Test
    public void test_serialization() {
        PubsubPullResponse pullResponse = PubsubPullResponseFixture.createRandomPullResponse(30);
        String pullResponseJson = JacksonHelpers.writeValueAsString(pullResponse);
        PubsubPullResponse deserializedPullResponse =
                JacksonHelpers.readValue(pullResponseJson, PubsubPullResponse.class);
        Assertions.assertThat(deserializedPullResponse).isEqualTo(pullResponse);
    }

}
