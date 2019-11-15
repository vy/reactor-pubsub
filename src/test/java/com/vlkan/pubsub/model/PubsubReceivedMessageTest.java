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

public class PubsubReceivedMessageTest {

    private static final Instant PUBLISH_INSTANT = Instant.parse("2019-08-27T11:10:00.910Z");

    private static final String ID = "id1";

    private static final byte[] PAYLOAD = new byte[]{1, 2, 3};

    private static final String ENCODED_PAYLOAD = Base64.getEncoder().encodeToString(PAYLOAD);

    @Test
    public void test_deserialization_with_empty_publishInstant() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.ID + "\": \"" + ID + '"' +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'publishTime'");
    }

    @Test
    public void test_deserialization_with_null_publishInstant() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT + "\": null" +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.ID + "\": \"" + ID + '"' +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: publishInstant");
    }

    @Test
    public void test_deserialization_with_empty_id() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'messageId'");
    }

    @Test
    public void test_deserialization_with_null_id() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.ID + "\": null" +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
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
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.ID + "\": \"" + ID + '"' +
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
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.ID + "\": \"" + ID + '"' +
                            ",\"" + PubsubReceivedMessage.JsonFieldName.PAYLOAD + "\": null" +
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
        Map<String, String> expectedAttributes = Collections.singletonMap("key", "val");
        Map<String, Object> expectedMessageMap = MapHelpers.createMap(
                PubsubReceivedMessage.JsonFieldName.PUBLISH_INSTANT, expectedInstant.toString(),
                PubsubReceivedMessage.JsonFieldName.ID, "messageId1",
                PubsubReceivedMessage.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload),
                PubsubReceivedMessage.JsonFieldName.ATTRIBUTES, expectedAttributes);
        String messageJson = JacksonHelpers.writeValueAsString(expectedMessageMap);

        // Deserialize Pub/Sub message from the JSON.
        PubsubReceivedMessage actualMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedMessage.class);

        // Build the expected response model.
        PubsubReceivedMessage expectedMessage =
                new PubsubReceivedMessage(
                        expectedInstant, "messageId1", expectedPayload, expectedAttributes);

        // Compare contents.
        Assertions.assertThat(actualMessage).isEqualTo(expectedMessage);

    }

    @Test
    public void test_serialization() {
        PubsubReceivedMessage message =
                PubsubReceivedMessageFixture.createRandomReceivedMessage();
        String messageJson = JacksonHelpers.writeValueAsString(message);
        PubsubReceivedMessage deserializedMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedMessage.class);
        Assertions.assertThat(deserializedMessage).isEqualTo(message);
    }

}
