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

import com.vlkan.pubsub.jackson.JacksonHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
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
    public void test_ctor_with_null_attributes() {
        Assertions
                .assertThatThrownBy(() -> new PubsubDraftedMessage(new byte[0], null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("attributes");
    }

    @Test
    public void test_serialization() {
        byte[] payload = new byte[]{1, 2, 3, 4};
        Map<String, String> attributes = Collections.singletonMap("key", "val");
        PubsubDraftedMessage message = new PubsubDraftedMessage(payload, attributes);
        Map<String, Object> actualMessageMap = JacksonHelpers.writeValueAsMap(message);
        Map<String, Object> expectedMessageMap = new LinkedHashMap<>();
        expectedMessageMap.put(
                PubsubDraftedMessage.JsonFieldName.PAYLOAD,
                Base64.getEncoder().encodeToString(payload));
        expectedMessageMap.put(
                PubsubDraftedMessage.JsonFieldName.ATTRIBUTES,
                attributes);
        Assertions.assertThat(actualMessageMap).isEqualTo(expectedMessageMap);
    }

}
