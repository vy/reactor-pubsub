/*
 * Copyright 2019 Volkan Yazıcı
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permits and
 * limitations under the License.
 */

package com.vlkan.pubsub.model;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.vlkan.pubsub.MapHelpers;
import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

public class PubsubReceivedMessageTest {

    @Test
    public void test_deserialization_with_empty_ackId() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{}";
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Missing required creator property 'ackId'");
    }

    @Test
    public void test_deserialization_with_null_ackId() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.ACK_ID + "\": null" +
                            ", \"" + PubsubReceivedMessage.JsonFieldName.EMBEDDING + "\": null" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("problem: ackId");
    }

    @Test
    public void test_deserialization_with_empty_message() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{\"" + PubsubReceivedMessage.JsonFieldName.ACK_ID + "\": \"id1\"}";
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Missing required creator property 'message'");
    }

    @Test
    public void test_deserialization_with_null_embedding() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessage.JsonFieldName.ACK_ID + "\": \"ackId1\"" +
                            ", \"" + PubsubReceivedMessage.JsonFieldName.EMBEDDING + "\": null" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessage.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("problem: embedding");
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload = {1, 2, 3};
        Map<String, String> expectedAttributes = Collections.singletonMap("key", "val");
        Map<String, Object> expectedReceivedMessageEmbeddingMap = MapHelpers.createMap(
                PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT, expectedInstant.toString(),
                PubsubReceivedMessageEmbedding.JsonFieldName.ID, "messageId1",
                PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload),
                PubsubReceivedMessageEmbedding.JsonFieldName.ATTRIBUTES, expectedAttributes);
        Map<String, Object> expectedReceivedMessageMap = MapHelpers.createMap(
                PubsubReceivedMessage.JsonFieldName.ACK_ID, "ackId1",
                PubsubReceivedMessage.JsonFieldName.EMBEDDING, expectedReceivedMessageEmbeddingMap);
        String messageJson = JacksonHelpers.writeValueAsString(expectedReceivedMessageMap);

        // Deserialize Pub/Sub message from the JSON.
        PubsubReceivedMessage actualMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedMessage.class);

        // Build the expected response model.
        PubsubReceivedMessage expectedMessage = new PubsubReceivedMessage(
                "ackId1",
                new PubsubReceivedMessageEmbedding(
                        expectedInstant, "messageId1", expectedPayload, expectedAttributes));

        // Compare contents.
        Assertions.assertThat(actualMessage).isEqualTo(expectedMessage);

    }

    @Test
    public void test_serialization() {
        PubsubReceivedMessage message = PubsubReceivedMessageFixture.createRandomReceivedMessage();
        String messageJson = JacksonHelpers.writeValueAsString(message);
        PubsubReceivedMessage deserializedMessage = JacksonHelpers.readValue(messageJson, PubsubReceivedMessage.class);
        Assertions.assertThat(deserializedMessage).isEqualTo(message);
    }

}
