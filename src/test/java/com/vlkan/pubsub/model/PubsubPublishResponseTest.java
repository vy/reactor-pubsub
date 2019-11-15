package com.vlkan.pubsub.model;

import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
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
                .hasCauseInstanceOf(MismatchedInputException.class)
                .hasMessageContaining("Missing required creator property 'messageIds'");
    }

    @Test
    public void test_deserialization_with_null_messageIds() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{\"" + PubsubPublishResponse.JsonFieldName.MESSAGE_IDS + "\": null}";
                    JacksonHelpers.readValue(json, PubsubPublishResponse.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
                .hasMessageContaining("problem: messageIds");
    }

    @Test
    public void test_deserialization_with_empty_messageIds() {
        Assertions
                .assertThatThrownBy(() -> {
                    String json = "{\"" + PubsubPublishResponse.JsonFieldName.MESSAGE_IDS + "\": []}";
                    JacksonHelpers.readValue(json, PubsubPublishResponse.class);
                })
                .hasCauseInstanceOf(InvalidDefinitionException.class)
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
