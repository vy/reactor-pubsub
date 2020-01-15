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

package com.vlkan.pubsub.jackson;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class JacksonBase64EncodedStringDeserializerTest {

    private static final class TestModel {

        @SuppressWarnings("unused")
        @JsonDeserialize(using = JacksonBase64EncodedStringDeserializer.class)
        private byte[] bytes;

        private TestModel() {}

    }

    @Test
    public void test_null_bytes() {
        test(null);
    }

    @Test
    public void test_empty_bytes() {
        test(new byte[0]);
    }

    @Test
    public void test_non_empty_bytes() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int trialCount = 1_000;
        int maxByteCount = 1_000;
        for (int trialIndex = 0; trialIndex < trialCount; trialIndex++) {
            int byteCount = random.nextInt(maxByteCount);
            byte[] bytes = new byte[byteCount];
            for (int byteIndex = 0; byteIndex < byteCount; byteIndex++) {
                bytes[byteIndex] = (byte) random.nextInt(256);
            }
            test(bytes);
        }
    }

    private void test(@Nullable byte[] expectedBytes) {
        @Nullable String expectedEncodedBytes = expectedBytes != null
                ? Base64.getEncoder().encodeToString(expectedBytes)
                : null;
        String json = JacksonHelpers.writeValueAsString(
                Collections.singletonMap("bytes", expectedEncodedBytes));
        TestModel testModel = JacksonHelpers.readValue(json, TestModel.class);
        byte[] actualBytes = testModel.bytes;
        Assertions.assertThat(actualBytes).isEqualTo(expectedBytes);
    }

}
