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
