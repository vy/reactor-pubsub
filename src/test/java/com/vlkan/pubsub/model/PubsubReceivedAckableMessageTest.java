package com.vlkan.pubsub.model;

import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.vlkan.pubsub.MapHelpers;
import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.Map;

public class PubsubReceivedAckableMessageTest {

    @Test
    public void test_deserialization_with_empty_ackId() {
        Assertions
                .assertThatThrownBy(() -> JacksonHelpers.readValue(
                        "{}", PubsubReceivedAckableMessage.class))
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'ackId'");
    }

    @Test
    public void test_deserialization_with_null_ackId() {
        Assertions
                .assertThatThrownBy(() -> JacksonHelpers.readValue(
                        "{\"ackId\": null, \"message\": null}", PubsubReceivedAckableMessage.class))
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: ackId");
    }

    @Test
    public void test_deserialization_with_empty_message() {
        Assertions
                .assertThatThrownBy(() -> JacksonHelpers.readValue(
                        "{\"ackId\": \"id1\"}", PubsubReceivedAckableMessage.class))
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'message'");
    }

    @Test
    public void test_deserialization_with_null_message() {
        Assertions
                .assertThatThrownBy(() -> JacksonHelpers.readValue(
                        "{\"ackId\": \"id1\", \"message\": null}", PubsubReceivedAckableMessage.class))
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: message");
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload = {1, 2, 3};
        Map<String, Object> expectedReceivedMessageMap = MapHelpers.createMap(
                "publishTime", expectedInstant.toString(),
                "messageId", "messageId1",
                "data", Base64.getEncoder().encodeToString(expectedPayload));
        Map<String, Object> expectedReceivedAckableMessageMap = MapHelpers.createMap(
                "ackId", "ackId1",
                "message", expectedReceivedMessageMap);
        String messageJson = JacksonHelpers.writeValueAsString(expectedReceivedAckableMessageMap);

        // Deserialize Pub/Sub message from the JSON.
        PubsubReceivedAckableMessage actualMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedAckableMessage.class);

        // Build the expected response model.
        PubsubReceivedAckableMessage expectedMessage = new PubsubReceivedAckableMessage(
                "ackId1",
                new PubsubReceivedMessage(
                        expectedInstant, "messageId1", expectedPayload));

        // Compare contents.
        Assertions.assertThat(actualMessage).isEqualTo(expectedMessage);

    }

}
