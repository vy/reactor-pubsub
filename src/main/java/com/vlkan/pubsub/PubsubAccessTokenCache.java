/*
 * Copyright 2019 Volkan Yazıcı
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

package com.vlkan.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.vlkan.pubsub.util.BoundedScheduledThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class PubsubAccessTokenCache {

    private static final class DefaultExecutorServiceHolder {

        private static final ScheduledExecutorService INSTANCE =
                new BoundedScheduledThreadPoolExecutor(
                        100,
                        new ScheduledThreadPoolExecutor(1, new ThreadFactory() {

                            private final AtomicInteger threadCounter = new AtomicInteger(0);

                            @Override
                            public Thread newThread(Runnable runnable) {
                                String name = String.format(
                                        "PubsubAccessTokenCacheWorker-%02d",
                                        threadCounter.incrementAndGet());
                                return new Thread(runnable, name);
                            }

                        }));

    }

    public static final Supplier<ScheduledExecutorService> DEFAULT_EXECUTOR_SERVICE_SUPPLIER =
            () -> DefaultExecutorServiceHolder.INSTANCE;

    public static final Duration DEFAULT_ACCESS_TOKEN_REFRESH_PERIOD = Duration.ofMinutes(1);

    private static final class DefaultAccessTokenCacheHolder {

        private static final PubsubAccessTokenCache INSTANCE =
                PubsubAccessTokenCache.builder().build();

    }

    public static final Supplier<PubsubAccessTokenCache> DEFAULT_ACCESS_TOKEN_CACHE_SUPPLIER =
            () -> DefaultAccessTokenCacheHolder.INSTANCE;

    private static final Logger LOGGER = LoggerFactory.getLogger(PubsubAccessTokenCache.class);

    private static final String CLOUD_PLATFORM_URI = "https://www.googleapis.com/auth/cloud-platform";

    private static final String PUBSUB_URI = "https://www.googleapis.com/auth/pubsub";

    private static final List<String> SCOPES = Arrays.asList(CLOUD_PLATFORM_URI, PUBSUB_URI);

    private final GoogleCredentials credentials;

    private volatile String accessToken;

    private PubsubAccessTokenCache(Builder builder) {
        this.credentials = createGoogleCredentials(builder.credentials);
        long accessTokenRefreshPeriodMillis = builder.accessTokenRefreshPeriod.toMillis();
        builder.executorService.scheduleAtFixedRate(
                this::refreshAccessToken,
                accessTokenRefreshPeriodMillis,
                accessTokenRefreshPeriodMillis,
                TimeUnit.MILLISECONDS);
    }

    String getAccessToken() {
        return accessToken;
    }

    private static GoogleCredentials createGoogleCredentials(@Nullable String credentials) {
        GoogleCredentials googleCredentials;
        if (credentials == null) {
            try {
                googleCredentials = GoogleCredentials.getApplicationDefault();
            } catch (IOException error) {
                throw new RuntimeException("cannot retrieve default credentials", error);
            }
        } else {
            Objects.requireNonNull(credentials, "credentials");
            googleCredentials = createServiceAccountCredentials(credentials);
        }
        return googleCredentials.createScopedRequired()
                ? googleCredentials.createScoped(SCOPES)
                : googleCredentials;
    }

    private static ServiceAccountCredentials createServiceAccountCredentials(String credentials) {
        byte[] credentialsBytes = Base64.getDecoder().decode(credentials);
        try (InputStream secretBytesStream = new ByteArrayInputStream(credentialsBytes)) {
            return ServiceAccountCredentials.fromStream(secretBytesStream);
        } catch (IOException error) {
            throw new RuntimeException(error);
        }
    }

    private void refreshAccessToken() {
        try {
            credentials.refreshIfExpired();
            this.accessToken = credentials.getAccessToken().getTokenValue();
        } catch (IOException error) {
            LOGGER.error("Failed to fetch access token", error);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private ScheduledExecutorService executorService;

        private Duration accessTokenRefreshPeriod = DEFAULT_ACCESS_TOKEN_REFRESH_PERIOD;

        @Nullable
        private String credentials;

        private Builder() {}

        public Builder setExecutorService(ScheduledExecutorService executorService) {
            this.executorService = Objects.requireNonNull(executorService, "executorService");
            return this;
        }

        public Builder setAccessTokenRefreshPeriod(Duration accessTokenRefreshPeriod) {
            this.accessTokenRefreshPeriod = Objects.requireNonNull(accessTokenRefreshPeriod, "accessTokenRefreshPeriod");
            return this;
        }

        public Builder setCredentials(@Nullable String credentials) {
            this.credentials = credentials;
            return this;
        }

        public PubsubAccessTokenCache build() {
            if (executorService == null) {
                executorService = DEFAULT_EXECUTOR_SERVICE_SUPPLIER.get();
            }
            Objects.requireNonNull(accessTokenRefreshPeriod, "accessTokenRefreshPeriod");
            return new PubsubAccessTokenCache(this);
        }

    }

}
