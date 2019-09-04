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

public class PubsubReceivedMessageTest {

    private static final Instant PUBLISH_TIME_INSTANT = Instant.parse("2019-08-27T11:10:00.910Z");

    private static final String MESSAGE_ID = "id1";

    private static final byte[] DATA_BYTES = new byte[]{1, 2, 3};

    private static final String DATA_STRING = Base64.getEncoder().encodeToString(DATA_BYTES);

    @Test
    public void test_deserialization_with_empty_publishTime() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = '{' +
                            "\"messageId\": \"" + MESSAGE_ID + "\"" +
                            ",\"data\": \"" + DATA_STRING + "\"" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'publishTime'");
    }

    @Test
    public void test_deserialization_with_null_publishTime() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = '{' +
                            "\"publishTime\": null" +
                            ",\"messageId\": \"" + MESSAGE_ID + "\"" +
                            ",\"data\": \"" + DATA_STRING + "\"" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: publishInstant");
    }

    @Test
    public void test_deserialization_with_empty_messageId() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = '{' +
                            "\"publishTime\": " + PUBLISH_TIME_INSTANT.toEpochMilli() +
                            ",\"data\": \"" + DATA_STRING + "\"" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'messageId'");
    }

    @Test
    public void test_deserialization_with_null_messageId() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = '{' +
                            "\"publishTime\": " + PUBLISH_TIME_INSTANT.toEpochMilli() +
                            ",\"messageId\": null" +
                            ",\"data\": \"" + DATA_STRING + "\"" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: id");
    }

    @Test
    public void test_deserialization_with_empty_data() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = '{' +
                            "\"publishTime\": " + PUBLISH_TIME_INSTANT.toEpochMilli() +
                            ",\"messageId\": \"" + MESSAGE_ID + "\"" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'data'");
    }

    @Test
    public void test_deserialization_with_null_data() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = '{' +
                            "\"publishTime\": " + PUBLISH_TIME_INSTANT.toEpochMilli() +
                            ",\"messageId\": \"" + MESSAGE_ID + "\"" +
                            ",\"data\": null" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: payload");
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload = {1, 2, 3};
        Map<String, Object> expectedMessageMap = MapHelpers.createMap(
                "publishTime", expectedInstant.toEpochMilli(),
                "messageId", "messageId1",
                "data", Base64.getEncoder().encodeToString(expectedPayload));
        String messageJson = JacksonHelpers.writeValueAsString(expectedMessageMap);

        // Deserialize Pub/Sub message from the JSON.
        PubsubReceivedMessage actualMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedMessage.class);

        // Build the expected response model.
        PubsubReceivedMessage expectedMessage =
                new PubsubReceivedMessage(expectedInstant, "messageId1", expectedPayload);

        // Compare contents.
        Assertions.assertThat(actualMessage).isEqualTo(expectedMessage);

    }

}
