package com.vlkan.pubsub.model;

import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PubsubAckRequestTest {

    @Test
    public void test_ctor_with_null_ackIds() {
        Assertions
                .assertThatThrownBy(() -> new PubsubAckRequest(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("ackIds");
    }

    @Test
    public void test_ctor_with_empty_ackIds() {
        Assertions
                .assertThatThrownBy(() -> new PubsubAckRequest(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("empty ackIds");
    }

    @Test
    public void test_serialization() {
        List<String> ackIds = Collections.singletonList("some-ack-id");
        PubsubAckRequest request = new PubsubAckRequest(ackIds);
        Map<String, Object> actualRequestMap = JacksonHelpers.writeValueAsMap(request);
        Map<String, Object> expectedRequestMap = Collections.singletonMap("ackIds", ackIds);
        Assertions.assertThat(actualRequestMap).isEqualTo(expectedRequestMap);
    }

}
