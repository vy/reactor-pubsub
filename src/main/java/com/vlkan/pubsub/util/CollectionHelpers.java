package com.vlkan.pubsub.util;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum CollectionHelpers {;

    public static String limitedFormat(Collection<?> items, int maxVisibleItemCount) {
        int itemCount = items.size();
        Collection<?> visibleItems = items;
        if (itemCount > maxVisibleItemCount) {
            visibleItems = Stream
                    .concat(items.stream().limit(maxVisibleItemCount),
                            Stream.of(String.format("<%d more>", itemCount - maxVisibleItemCount)))
                    .collect(Collectors.toList());
        }
        return String.valueOf(visibleItems);
    }

}
