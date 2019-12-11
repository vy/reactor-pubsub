package com.vlkan.pubsub.util;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class CollectionHelpersTest {

    private static final class LimitedFormatTestCase {

        private final Collection<?> items;

        private final String expectedFormat;

        private LimitedFormatTestCase(Collection<?> items, String expectedFormat) {
            this.items = items;
            this.expectedFormat = expectedFormat;
        }

    }

    @Test
    public void test_limitedFormat() {
        int maxVisibleItemCount = 2;
        Arrays
                .asList(new LimitedFormatTestCase(Collections.emptyList(), "[]"),
                        new LimitedFormatTestCase(Collections.singletonList(1), "[1]"),
                        new LimitedFormatTestCase(Arrays.asList(1, 2), "[1, 2]"),
                        new LimitedFormatTestCase(Arrays.asList(1, 2, 3), "[1, 2, <1 more>]"),
                        new LimitedFormatTestCase(Arrays.asList(1, 2, 3, 4), "[1, 2, <2 more>]"))
                .forEach(testCase -> Assertions
                        .assertThat(CollectionHelpers.limitedFormat(testCase.items, maxVisibleItemCount))
                        .as("items=%s, expectedFormat=%s", testCase.items, testCase.expectedFormat)
                        .isEqualTo(testCase.expectedFormat));
    }

}
