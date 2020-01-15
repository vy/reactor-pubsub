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
            expectedRequestMap.put(PubsubPullRequest.JsonFieldName.IMMEDIATE_RETURN_ENABLED, immediateReturnEnabled);
            expectedRequestMap.put(PubsubPullRequest.JsonFieldName.MAX_MESSAGE_COUNT, maxMessageCount);
            PubsubPullRequest request = new PubsubPullRequest(immediateReturnEnabled, maxMessageCount);
            Map<String, Object> actualRequestMap = JacksonHelpers.writeValueAsMap(request);
            Assertions.assertThat(actualRequestMap).isEqualTo(expectedRequestMap);
        }
    }

}
