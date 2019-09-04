package com.vlkan.pubsub.model;

import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class PubsubPullRequestTest {

    @Test
    public void test_ctor_with_invalid_maxMessageCount() {
        for (int maxMessageCount : new int[]{-2, -1, 0}) {
            Assertions
                    .assertThatThrownBy(() -> new PubsubPullRequest(true, maxMessageCount))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("was expecting a positive non-zero maxMessageCount");
        }
    }

    @Test
    public void test_serialization() {
        int maxMessageCount = 1;
        for (boolean immediateReturnEnabled : new boolean[]{false, true}) {
            Map<String, Object> expectedRequestMap = new LinkedHashMap<>();
            expectedRequestMap.put("returnImmediately", immediateReturnEnabled);
            expectedRequestMap.put("maxMessages", maxMessageCount);
            PubsubPullRequest request = new PubsubPullRequest(immediateReturnEnabled, maxMessageCount);
            Map<String, Object> actualRequestMap = JacksonHelpers.writeValueAsMap(request);
            Assertions.assertThat(actualRequestMap).isEqualTo(expectedRequestMap);
        }
    }

}
