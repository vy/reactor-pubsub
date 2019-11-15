package com.vlkan.pubsub;

import java.util.LinkedHashMap;
import java.util.Map;

public enum MapHelpers {;

    public static <K, V> Map<K, V> createMap(K key1, V value1, K key2, V value2) {
        LinkedHashMap<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    public static <K, V> Map<K, V> createMap(K key1, V value1, K key2, V value2, K key3, V value3) {
        LinkedHashMap<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }

    public static <K, V> Map<K, V> createMap(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        LinkedHashMap<K, V> map = new LinkedHashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        return map;
    }

}
