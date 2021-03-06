/*
 * Copyright 2019-2020 Volkan Yazıcı
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
import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PubsubPublishResponseTest {

    @Test
    public void test_deserialization_with_empty_json() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{}";
                    JacksonHelpers.readValue(json, PubsubPublishResponse.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("Missing required creator property 'messageIds'");
    }

    @Test
    public void test_deserialization_with_null_messageIds() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{\"" + PubsubPublishResponse.JsonFieldName.MESSAGE_IDS + "\": null}";
                    JacksonHelpers.readValue(json, PubsubPublishResponse.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("problem: messageIds");
    }

    @Test
    public void test_deserialization_with_empty_messageIds() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{\"" + PubsubPublishResponse.JsonFieldName.MESSAGE_IDS + "\": []}";
                    JacksonHelpers.readValue(json, PubsubPublishResponse.class);
                })
                .hasCauseInstanceOf(JsonMappingException.class)
                .hasMessageContaining("problem: emtpy messageIds");
    }

    @Test
    public void test_deserialization() {
        List<String> expectedMessageIds = Arrays.asList("id1", "id2");
        Map<String, Object> expectedResponseMap = Collections.singletonMap(
                PubsubPublishResponse.JsonFieldName.MESSAGE_IDS,
                expectedMessageIds);
        String responseJson = JacksonHelpers.writeValueAsString(expectedResponseMap);
        PubsubPublishResponse actualResponse =
                JacksonHelpers.readValue(responseJson, PubsubPublishResponse.class);
        List<String> actualMessageIds = actualResponse.getMessageIds();
        Assertions.assertThat(actualMessageIds).isEqualTo(expectedMessageIds);
    }

    @Test
    public void test_serialization() {
        PubsubPublishResponse response =
                new PubsubPublishResponse(Arrays.asList("message1", "message2"));
        String responseJson = JacksonHelpers.writeValueAsString(response);
        PubsubPublishResponse deserializedResponse =
                JacksonHelpers.readValue(responseJson, PubsubPublishResponse.class);
        Assertions.assertThat(deserializedResponse).isEqualTo(response);
    }

}
