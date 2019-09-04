package com.vlkan.pubsub.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public enum JacksonHelpers {;

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> T readValue(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (IOException error) {
            throw new RuntimeException(error);
        }
    }

    public static String writeValueAsString(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException error) {
            throw new RuntimeException(error);
        }
    }

    public static Map<String, Object> writeValueAsMap(Object object) {
        String json = writeValueAsString(object);
        // noinspection unchecked
        return readValue(json, Map.class);
    }

}
