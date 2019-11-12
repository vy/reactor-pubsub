package com.vlkan.pubsub;

import org.mockito.Mockito;

public enum PubsubAccessTokenCacheFixture {;

    private static final PubsubAccessTokenCache INSTANCE = instantiate();

    private static PubsubAccessTokenCache instantiate() {
        PubsubAccessTokenCache instance = Mockito.mock(PubsubAccessTokenCache.class);
        Mockito.when(instance.getAccessToken()).thenReturn("test");
        return instance;
    }

    public static PubsubAccessTokenCache getInstance() {
        return INSTANCE;
    }

}
