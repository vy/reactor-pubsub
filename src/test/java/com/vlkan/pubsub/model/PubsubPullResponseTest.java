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
        Assertions.assertThat(response.getReceivedMessages()).isEmpty();
    }

    @Test
    public void test_deserialization_with_null_receivedMessages() {
        String responseJson = "{\"" + PubsubPullResponse.JsonFieldName.RECEIVED_MESSAGES + "\": null}";
        PubsubPullResponse response = JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);
        Assertions.assertThat(response.getReceivedMessages()).isEmpty();
    }

    @Test
    public void test_deserialization_with_empty_receivedMessages() {
        String responseJson = "{\"" + PubsubPullResponse.JsonFieldName.RECEIVED_MESSAGES + "\": []}";
        PubsubPullResponse response = JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);
        Assertions.assertThat(response.getReceivedMessages()).isEmpty();
    }

    @Test
    public void test_deserialization() {

        // Build a Pub/Sub pull response JSON.
        Instant expectedInstant1 = Instant.parse("2019-08-27T08:04:57Z");
        byte[] expectedPayload1 = {1, 2, 3};
        Map<String, String> expectedAttributes1 = Collections.singletonMap("key1", "val1");
        Map<String, Object> expectedReceivedMessageEmbedding1Map = MapHelpers.createMap(
                PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT, expectedInstant1.toString(),
                PubsubReceivedMessageEmbedding.JsonFieldName.ID, "messageId1",
                PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload1),
                PubsubReceivedMessageEmbedding.JsonFieldName.ATTRIBUTES, expectedAttributes1);
        Map<String, Object> expectedReceivedMessage1Map = MapHelpers.createMap(
                PubsubReceivedMessage.JsonFieldName.ACK_ID, "ackId1",
                PubsubReceivedMessage.JsonFieldName.EMBEDDING, expectedReceivedMessageEmbedding1Map);
        Instant expectedInstant2 = expectedInstant1.plus(Duration.ofMinutes(1));
        byte[] expectedPayload2 = {4, 5, 6};
        Map<String, String> expectedAttributes2 = Collections.singletonMap("key2", "val2");
        Map<String, Object> expectedReceivedMessageEmbedding2Map = MapHelpers.createMap(
                PubsubReceivedMessageEmbedding.JsonFieldName.PUBLISH_INSTANT, expectedInstant2.toString(),
                PubsubReceivedMessageEmbedding.JsonFieldName.ID, "messageId2",
                PubsubReceivedMessageEmbedding.JsonFieldName.PAYLOAD, Base64.getEncoder().encodeToString(expectedPayload2),
                PubsubReceivedMessageEmbedding.JsonFieldName.ATTRIBUTES, expectedAttributes2);
        Map<String, Object> expectedReceivedMessage2Map = MapHelpers.createMap(
                PubsubReceivedMessage.JsonFieldName.ACK_ID, "ackId2",
                PubsubReceivedMessage.JsonFieldName.EMBEDDING, expectedReceivedMessageEmbedding2Map);
        List<Map<String, Object>> expectedReceivedMessagesList = Arrays.asList(
                expectedReceivedMessage1Map,
                expectedReceivedMessage2Map);
        Map<String, Object> expectedResponseMap = Collections.singletonMap(
                PubsubPullResponse.JsonFieldName.RECEIVED_MESSAGES,
                expectedReceivedMessagesList);
        String responseJson = JacksonHelpers.writeValueAsString(expectedResponseMap);

        // Deserialize Pub/Sub pull response from the JSON.
        PubsubPullResponse actualResponse =
                JacksonHelpers.readValue(responseJson, PubsubPullResponse.class);

        // Build the expected response model.
        PubsubPullResponse expectedResponse = new PubsubPullResponse(Arrays.asList(
                new PubsubReceivedMessage(
                        "ackId1",
                        new PubsubReceivedMessageEmbedding(
                                expectedInstant1,
                                "messageId1",
                                expectedPayload1,
                                expectedAttributes1)),
                new PubsubReceivedMessage(
                        "ackId2",
                        new PubsubReceivedMessageEmbedding(
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
