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
