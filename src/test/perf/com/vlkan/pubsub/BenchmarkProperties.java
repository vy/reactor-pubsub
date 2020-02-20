package com.vlkan.pubsub;

enum BenchmarkProperties {;

    static String getStringProperty(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }

    static int getIntProperty(String key, int defaultValue) {
        String valueString = System.getProperty(key);
        return valueString != null
                ? Integer.parseInt(valueString)
                : defaultValue;
    }

    static long getLongProperty(String key, long defaultValue) {
        String valueString = System.getProperty(key);
        return valueString != null
                ? Long.parseLong(valueString)
                : defaultValue;
    }

}
