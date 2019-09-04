package com.vlkan.pubsub.jackson;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class JacksonBase64EncodedStringSerializerTest {

    private static final class TestModel {

        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        @JsonSerialize(using = JacksonBase64EncodedStringSerializer.class)
        private final byte[] bytes;

        private TestModel(byte[] bytes) {
            this.bytes = bytes;
        }

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
        @Nullable String expectedString = expectedBytes != null
                ? Base64.getEncoder().encodeToString(expectedBytes)
                : null;
        TestModel testModel = new TestModel(expectedBytes);
        Map<String, Object> actualMap = JacksonHelpers.writeValueAsMap(testModel);
        Map<String, Object> expectedMap = Collections.singletonMap("bytes", expectedString);
        Assertions.assertThat(actualMap).isEqualTo(expectedMap);
    }

}
