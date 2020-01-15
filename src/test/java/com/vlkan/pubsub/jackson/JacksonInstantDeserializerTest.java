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
import java.time.Instant;
import java.util.Collections;

public class JacksonInstantDeserializerTest {

    private static final class TestModel {

        @Nullable
        @SuppressWarnings("unused")
        @JsonDeserialize(using = JacksonInstantDeserializer.class)
        private Instant instant;

        private TestModel() {}

    }

    @Test
    public void test_null_instant() {
        test(null);
    }

    @Test
    public void test_instant() {
        test(Instant.parse("2019-08-27T08:41:57.719Z"));
    }

    private void test(@Nullable Instant expectedInstant) {
        String expectedInstantString = expectedInstant != null
                ? expectedInstant.toString()
                : null;
        String json = JacksonHelpers.writeValueAsString(
                Collections.singletonMap("instant", expectedInstantString));
        TestModel testModel = JacksonHelpers.readValue(json, TestModel.class);
        @Nullable Instant actualInstant = testModel.instant;
        Assertions.assertThat(actualInstant).isEqualTo(expectedInstant);
    }

}
