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
        Long expectedInstantMillis = expectedInstant != null
                ? expectedInstant.toEpochMilli()
                : null;
        String json = JacksonHelpers.writeValueAsString(
                Collections.singletonMap("instant", expectedInstantMillis));
        TestModel testModel = JacksonHelpers.readValue(json, TestModel.class);
        @Nullable Instant actualInstant = testModel.instant;
        Assertions.assertThat(actualInstant).isEqualTo(expectedInstant);
    }

}
