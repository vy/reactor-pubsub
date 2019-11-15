package com.vlkan.pubsub.model;

import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.vlkan.pubsub.MapHelpers;
import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

public class PubsubReceivedAckableMessageTest {

    @Test
    public void test_deserialization_with_empty_ackId() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{}";
                    JacksonHelpers.readValue(json, PubsubReceivedAckableMessage.class);
                })
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'ackId'");
    }

    @Test
    public void test_deserialization_with_null_ackId() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedAckableMessage.JsonFieldName.ACK_ID + "\": null" +
                            ", \"" + PubsubReceivedAckableMessage.JsonFieldName.MESSAGE + "\": null" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedAckableMessage.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: ackId");
    }

    @Test
    public void test_deserialization_with_empty_message() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{\"" + PubsubReceivedAckableMessage.JsonFieldName.ACK_ID + "\": \"id1\"}";
                    JacksonHelpers.readValue(json, PubsubReceivedAckableMessage.class);
                })
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'message'");
    }

    @Test
    public void test_deserialization_with_null_message() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedAckableMessage.JsonFieldName.ACK_ID + "\": \"ackId1\"" +
                            ", \"" + PubsubReceivedAckableMessage.JsonFieldName.MESSAGE + "\": null" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedAckableMessage.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: message");
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload = {1, 2, 3};
        Map<String, String> expectedAttributes = Collections.singletonMap("key", "val");
        Map<String, Object> expectedReceivedMessageMap = MapHelpers.createMap(
                PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT, expectedInstant.toString(),
                PubsubReceivedMessage.JsonFieldName.ID, "messageId1",
                PubsubReceivedMessage.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload),
                PubsubReceivedMessage.JsonFieldName.ATTRIBUTES, expectedAttributes);
        Map<String, Object> expectedReceivedAckableMessageMap = MapHelpers.createMap(
                PubsubReceivedAckableMessage.JsonFieldName.ACK_ID, "ackId1",
                PubsubReceivedAckableMessage.JsonFieldName.MESSAGE, expectedReceivedMessageMap);
        String messageJson = JacksonHelpers.writeValueAsString(expectedReceivedAckableMessageMap);

        // Deserialize Pub/Sub message from the JSON.
        PubsubReceivedAckableMessage actualMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedAckableMessage.class);

        // Build the expected response model.
        PubsubReceivedAckableMessage expectedMessage = new PubsubReceivedAckableMessage(
                "ackId1",
                new PubsubReceivedMessage(
                        expectedInstant, "messageId1", expectedPayload, expectedAttributes));

        // Compare contents.
        Assertions.assertThat(actualMessage).isEqualTo(expectedMessage);

    }

    @Test
    public void test_serialization() {
        PubsubReceivedAckableMessage ackableMessage =
                PubsubReceivedAckableMessageFixture.createRandomReceivedAckableMessage();
        String ackableMessageJson = JacksonHelpers.writeValueAsString(ackableMessage);
        PubsubReceivedAckableMessage deserializedAckableMessage =
                JacksonHelpers.readValue(ackableMessageJson, PubsubReceivedAckableMessage.class);
        Assertions.assertThat(deserializedAckableMessage).isEqualTo(ackableMessage);
    }

}
