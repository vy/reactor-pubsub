package com.vlkan.pubsub.model;

import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.Map;

public class PubsubDraftedMessageTest {

    @Test
    public void test_ctor_with_null_payload() {
        Assertions
                .assertThatThrownBy(() -> new PubsubDraftedMessage(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("payload");
    }

    @Test
    public void test_serialization() {
        byte[] payload = new byte[]{1, 2, 3, 4};
        PubsubDraftedMessage message = new PubsubDraftedMessage(payload);
        Map<String, Object> actualMessageMap = JacksonHelpers.writeValueAsMap(message);
        Map<String, Object> expectedMessageMap = Collections.singletonMap(
                "data", Base64.getEncoder().encodeToString(payload));
        Assertions.assertThat(actualMessageMap).isEqualTo(expectedMessageMap);
    }

}
