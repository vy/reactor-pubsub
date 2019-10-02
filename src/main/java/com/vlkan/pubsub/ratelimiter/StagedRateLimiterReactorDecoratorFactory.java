package com.vlkan.pubsub.ratelimiter;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class StagedRateLimiterReactorDecoratorFactory {

    public static final String DEFAULT_METER_NAME = "pubsub.stagedRateLimiter";

    public static final Map<String, String> DEFAULT_METER_TAGS = Collections.emptyMap();

    private final StagedRateLimiter stagedRateLimiter;

    @Nullable
    private final Scheduler scheduler;

    @Nullable
    private final DistributionSummary permitWaitPeriodDistributions;

    private StagedRateLimiterReactorDecoratorFactory(Builder builder) {
        this.stagedRateLimiter = builder.stagedRateLimiter;
        this.scheduler = builder.scheduler;
        if (builder.meterRegistry == null) {
            permitWaitPeriodDistributions = null;
        } else {
            DistributionSummary.Builder meterBuilder = DistributionSummary
                    .builder(builder.meterName + ".permitWaitPeriod")
                    .tag("type", "summary")
                    .tag("name", stagedRateLimiter.getName());
            builder.meterTags.forEach(meterBuilder::tag);
            permitWaitPeriodDistributions = meterBuilder.register(builder.meterRegistry);
        }
    }

    public StagedRateLimiter getStagedRateLimiter() {
        return stagedRateLimiter;
    }

    @Nullable
    public Scheduler getScheduler() {
        return scheduler;
    }

    public <V> Function<Mono<V>, Mono<V>> ofMono() {
        return (Mono<V> mono) -> mono
                .delayUntil(value -> {
                    long permitWaitPeriodNanos = stagedRateLimiter.nextPermitWaitPeriodNanos();
                    if (permitWaitPeriodDistributions != null) {
                        permitWaitPeriodDistributions.record(permitWaitPeriodNanos);
                    }
                    if (permitWaitPeriodNanos > 0L) {
                        return scheduler == null
                                ? Mono.delay(Duration.ofNanos(permitWaitPeriodNanos))
                                : Mono.delay(Duration.ofNanos(permitWaitPeriodNanos), scheduler);
                    } else {
                        return Mono.empty();
                    }
                })
                .doOnError(ignored -> stagedRateLimiter.acknowledgeFailure());
    }

    public <V> Function<Flux<V>, Flux<V>> ofFlux() {
        return (Flux<V> flux) -> flux
                .delayUntil(value -> {
                    long permitWaitPeriodNanos = stagedRateLimiter.nextPermitWaitPeriodNanos();
                    if (permitWaitPeriodDistributions != null) {
                        permitWaitPeriodDistributions.record(permitWaitPeriodNanos);
                    }
                    if (permitWaitPeriodNanos > 0L) {
                        return scheduler == null
                                ? Mono.delay(Duration.ofNanos(permitWaitPeriodNanos))
                                : Mono.delay(Duration.ofNanos(permitWaitPeriodNanos), scheduler);
                    } else {
                        return Mono.empty();
                    }
                })
                .doOnError(ignored -> stagedRateLimiter.acknowledgeFailure());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private StagedRateLimiter stagedRateLimiter;

        @Nullable
        private Scheduler scheduler;

        @Nullable
        private MeterRegistry meterRegistry;

        private String meterName = DEFAULT_METER_NAME;

        private Map<String, String> meterTags = DEFAULT_METER_TAGS;

        public Builder setStagedRateLimiter(StagedRateLimiter stagedRateLimiter) {
            this.stagedRateLimiter = Objects.requireNonNull(stagedRateLimiter, "stagedRateLimiter");
            return this;
        }

        public Builder setScheduler(@Nullable Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder setMeterRegistry(@Nullable MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            return this;
        }

        public Builder setMeterName(String meterName) {
            this.meterName = Objects.requireNonNull(meterName, "meterName");
            return this;
        }

        public Builder setMeterTags(Map<String, String> meterTags) {
            this.meterTags = Objects.requireNonNull(meterTags, "meterTags");
            return this;
        }

        public StagedRateLimiterReactorDecoratorFactory build() {
            Objects.requireNonNull(stagedRateLimiter, "stagedRateLimiter");
            return new StagedRateLimiterReactorDecoratorFactory(this);
        }

    }

}
