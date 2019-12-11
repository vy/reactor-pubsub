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

public class PubsubReceivedMessageEmbeddingTest {

    private static final Instant PUBLISH_INSTANT = Instant.parse("2019-08-27T11:10:00.910Z");

    private static final String ID = "id1";

    private static final byte[] PAYLOAD = new byte[]{1, 2, 3};

    private static final String ENCODED_PAYLOAD = Base64.getEncoder().encodeToString(PAYLOAD);

    @Test
    public void test_deserialization_with_empty_publishInstant() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessageEmbedding.JsonFieldName.ID + "\": \"" + ID + '"' +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessageEmbedding.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Missing required creator property 'publishTime'");
    }

    @Test
    public void test_deserialization_with_null_publishInstant() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT + "\": null" +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.ID + "\": \"" + ID + '"' +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessageEmbedding.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("problem: publishInstant");
    }

    @Test
    public void test_deserialization_with_empty_id() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessageEmbedding.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Missing required creator property 'messageId'");
    }

    @Test
    public void test_deserialization_with_null_id() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.ID + "\": null" +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD + "\": \"" + ENCODED_PAYLOAD + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessageEmbedding.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("problem: id");
    }

    @Test
    public void test_deserialization_with_empty_data() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.ID + "\": \"" + ID + '"' +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessageEmbedding.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Missing required creator property 'data'");
    }

    @Test
    public void test_deserialization_with_null_data() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{" +
                            '"' + PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT + "\": \"" + PUBLISH_INSTANT + '"' +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.ID + "\": \"" + ID + '"' +
                            ",\"" + PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD + "\": null" +
                            '}';
                    JacksonHelpers.readValue(json, PubsubReceivedMessageEmbedding.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("problem: payload");
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload = {1, 2, 3};
        Map<String, String> expectedAttributes = Collections.singletonMap("key", "val");
        Map<String, Object> expectedMessageMap = MapHelpers.createMap(
                PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT, expectedInstant.toString(),
                PubsubReceivedMessageEmbedding.JsonFieldName.ID, "messageId1",
                PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload),
                PubsubReceivedMessageEmbedding.JsonFieldName.ATTRIBUTES, expectedAttributes);
        String messageJson = JacksonHelpers.writeValueAsString(expectedMessageMap);

        // Deserialize Pub/Sub message from the JSON.
        PubsubReceivedMessageEmbedding actualMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedMessageEmbedding.class);

        // Build the expected response model.
        PubsubReceivedMessageEmbedding expectedMessage =
                new PubsubReceivedMessageEmbedding(
                        expectedInstant, "messageId1", expectedPayload, expectedAttributes);

        // Compare contents.
        Assertions.assertThat(actualMessage).isEqualTo(expectedMessage);

    }

    @Test
    public void test_serialization() {
        PubsubReceivedMessageEmbedding message =
                PubsubReceivedMessageEmbeddingFixture.createRandomReceivedMessageEmbedding();
        String messageJson = JacksonHelpers.writeValueAsString(message);
        PubsubReceivedMessageEmbedding deserializedMessage =
                JacksonHelpers.readValue(messageJson, PubsubReceivedMessageEmbedding.class);
        Assertions.assertThat(deserializedMessage).isEqualTo(message);
    }

}
