package com.vlkan.pubsub.model;

import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PubsubPublishRequestTest {

    @Test
    public void test_ctor_with_null_messages() {
        Assertions
                .assertThatThrownBy(() -> new PubsubPublishRequest(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messages");
    }

    @Test
    public void test_ctor_with_empty_messages() {
        Assertions
                .assertThatThrownBy(() -> new PubsubPublishRequest(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("empty messages");
    }

    @Test
    public void test_serialization() {
        List<PubsubDraftedMessage> messages =
                Collections.singletonList(
                        new PubsubDraftedMessage(new byte[]{1, 2, 3, 4, 5}));
        PubsubPublishRequest request = new PubsubPublishRequest(messages);
        Map<String, Object> actualRequestMap = JacksonHelpers.writeValueAsMap(request);
        Map<String, Object> expectedRequestMap = Collections.singletonMap(
                PubsubPublishRequest.JsonFieldName.MESSAGES,
                messages.stream()
                        .map(JacksonHelpers::writeValueAsMap)
                        .collect(Collectors.toList()));
        Assertions.assertThat(actualRequestMap).isEqualTo(expectedRequestMap);
    }

}
