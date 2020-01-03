package com.vlkan.pubsub;

enum BenchmarkHelpers {;

    static String getStringProperty(String key, String defaultValue) {
        return System.getProperty(key, defaultValue);
    }

    static int getIntProperty(String key, int defaultValue) {
        String valueString = System.getProperty(key);
        return valueString != null
                ? Integer.parseInt(valueString)
                : defaultValue;
    }

}
