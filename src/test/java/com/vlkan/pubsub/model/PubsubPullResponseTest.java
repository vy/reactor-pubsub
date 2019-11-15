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
        String responseJson = "{}";
        PubsubPullResponse response = JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);
        Assertions.assertThat(response.getReceivedAckableMessages()).isEmpty();
    }

    @Test
    public void test_deserialization_with_null_receivedAckableMessages() {
        String responseJson = "{\"" + PubsubPullResponse.JsonFieldName.RECEIVED_ACKABLE_MESSAGES + "\": null}";
        PubsubPullResponse response = JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);
        Assertions.assertThat(response.getReceivedAckableMessages()).isEmpty();
    }

    @Test
    public void test_deserialization_with_empty_receivedAckableMessages() {
        String responseJson = "{\"" + PubsubPullResponse.JsonFieldName.RECEIVED_ACKABLE_MESSAGES + "\": []}";
        PubsubPullResponse response = JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);
        Assertions.assertThat(response.getReceivedAckableMessages()).isEmpty();
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant1 = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload1 = {1, 2, 3};
        Map<String, String> expectedAttributes1 = Collections.singletonMap("key1", "val1");
        Map<String, Object> expectedReceivedMessage1Map = MapHelpers.createMap(
                PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT, expectedInstant1.toString(),
                PubsubReceivedMessage.JsonFieldName.ID, "messageId1",
                PubsubReceivedMessage.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload1),
                PubsubReceivedMessage.JsonFieldName.ATTRIBUTES, expectedAttributes1);
        Map<String, Object> expectedReceivedAckableMessage1Map = MapHelpers.createMap(
                PubsubReceivedAckableMessage.JsonFieldName.ACK_ID, "ackId1",
                PubsubReceivedAckableMessage.JsonFieldName.MESSAGE, expectedReceivedMessage1Map);
        Instant expectedInstant2 = expectedInstant1.plus(Duration.ofMinutes(1));
        byte[] expectedPayload2 = {4, 5, 6};
        Map<String, String> expectedAttributes2 = Collections.singletonMap("key2", "val2");
        Map<String, Object> expectedReceivedMessage2Map = MapHelpers.createMap(
                PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT, expectedInstant2.toString(),
                PubsubReceivedMessage.JsonFieldName.ID, "messageId2",
                PubsubReceivedMessage.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload2),
                PubsubReceivedMessage.JsonFieldName.ATTRIBUTES, expectedAttributes2);
        Map<String, Object> expectedReceivedAckableMessage2Map = MapHelpers.createMap(
                PubsubReceivedAckableMessage.JsonFieldName.ACK_ID, "ackId2",
                PubsubReceivedAckableMessage.JsonFieldName.MESSAGE, expectedReceivedMessage2Map);
        List<Map<String, Object>> expectedReceivedAckableMessagesMap = Arrays.asList(
                expectedReceivedAckableMessage1Map,
                expectedReceivedAckableMessage2Map);
        Map<String, Object> expectedResponseMap = Collections.singletonMap(
                PubsubPullResponse.JsonFieldName.RECEIVED_ACKABLE_MESSAGES,
                expectedReceivedAckableMessagesMap);
        String responseJson = JacksonHelpers.writeValueAsString(expectedResponseMap);

        // Deserialize Pub/Sub pull response from the JSON.
        PubsubPullResponse actualResponse =
                JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);

        // Build the expected response model.
        PubsubPullResponse expectedResponse = new PubsubPullResponse(Arrays.asList(
                new PubsubReceivedAckableMessage(
                        "ackId1",
                        new PubsubReceivedMessage(
                                expectedInstant1,
                                "messageId1",
                                expectedPayload1,
                                expectedAttributes1)),
                new PubsubReceivedAckableMessage(
                        "ackId2",
                        new PubsubReceivedMessage(
                                expectedInstant2,
                                "messageId2",
                                expectedPayload2,
                                expectedAttributes2))));

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
