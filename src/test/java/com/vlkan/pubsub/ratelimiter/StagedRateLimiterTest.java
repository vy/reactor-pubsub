package com.vlkan.pubsub.ratelimiter;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;

public class StagedRateLimiterTest {

    @Test
    public void test_regex_violation() {
        Arrays
                .asList("",
                        ":",
                        ":,:",
                        ":,:,",
                        ",",
                        "1/1s:",
                        ",1/1s:1/1s",
                        "1/1s:",
                        "1/3s:, 1/2s:,",
                        "1/3s:, 1/2x:, :",
                        "2/1h:1/1h, 1/1m:, 4/1s:2/1s")
                .forEach(spec -> Assertions
                        .assertThatThrownBy(() -> StagedRateLimiter.of("test", spec))
                        .as("spec=%s", spec)
                        .hasCauseInstanceOf(IllegalArgumentException.class));
    }

    @Test
    public void test_failure_rate_limit_lower_than_one_for_success() {
        String spec = "1/1s:, 1/1h:2/1h, :";
        Assertions
                .assertThatThrownBy(() -> StagedRateLimiter.of("test", spec))
                .as("spec=%s", spec)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .satisfies(throwable -> Assertions
                        .assertThat(throwable.getCause())
                        .hasMessageContaining("was expecting failure rate limit to be lower than the one for success"));
    }

    @Test
    public void test_ascending_success_rate_order() {
        String spec = "1/1s:, 1/1h:, :";
        Assertions
                .assertThatThrownBy(() -> StagedRateLimiter.of("test", spec))
                .as("spec=%s", spec)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .satisfies(throwable -> Assertions
                        .assertThat(throwable.getCause())
                        .hasMessageContaining("was expecting rate limiters in ascending rate order"));
    }

    @Test
    public void test_fast_start() {
        StagedRateLimiter adaptiveRateLimiter = StagedRateLimiter.of("test", "1/1h:, 1/1m:, 4/1s:2/1s");
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("4/1s");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isEqualTo("2/1s");
    }

    @Test
    public void test_nack_slow_down_and_ack_speed_up_with_failure_rate_limits() {

        // Create an adaptive rate limiter where internal rate limiters are constantly failing.
        StagedRateLimiter.RateLimiterFactory constantlyFailingRateLimiterFactory =
                createConstantlyFailingRateLimiterFactory();
        StagedRateLimiter adaptiveRateLimiter =
                StagedRateLimiter.of(
                        "test",
                        "2/1h:, 2/1m:1/1m, 2/1s:1/1s",
                        constantlyFailingRateLimiterFactory);

        // Verify nack slow down.
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1s");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isEqualTo("1/1s");
        adaptiveRateLimiter.acknowledgeFailure();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1m");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isEqualTo("1/1m");
        adaptiveRateLimiter.acknowledgeFailure();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1h");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isNull();

        // Verify ack speed up.
        adaptiveRateLimiter.nextPermitWaitPeriodNanos();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1m");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isEqualTo("1/1m");
        adaptiveRateLimiter.nextPermitWaitPeriodNanos();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1s");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isEqualTo("1/1s");

    }

    @Test
    public void test_nack_slow_down_and_ack_speed_up_without_failure_rate_limits() {

        // Create an adaptive rate limiter where internal rate limiters are constantly failing.
        StagedRateLimiter.RateLimiterFactory constantlyFailingRateLimiterFactory =
                createConstantlyFailingRateLimiterFactory();
        StagedRateLimiter adaptiveRateLimiter =
                StagedRateLimiter.of(
                        "test",
                        "2/1h:, 2/1m:, 2/1s:",
                        constantlyFailingRateLimiterFactory);

        // Verify nack slow down.
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1s");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isNull();
        adaptiveRateLimiter.acknowledgeFailure();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1m");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isNull();
        adaptiveRateLimiter.acknowledgeFailure();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1h");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isNull();

        // Verify ack speed up.
        adaptiveRateLimiter.nextPermitWaitPeriodNanos();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1m");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isNull();
        adaptiveRateLimiter.nextPermitWaitPeriodNanos();
        Assertions.assertThat(adaptiveRateLimiter.getActiveSuccessRateLimitSpec()).isEqualTo("2/1s");
        Assertions.assertThat(adaptiveRateLimiter.getActiveFailureRateLimitSpec()).isNull();

    }

    private StagedRateLimiter.RateLimiterFactory createConstantlyFailingRateLimiterFactory() {
        return (maxPermitCountPerCycle, cyclePeriod) -> new RateLimiter(maxPermitCountPerCycle, cyclePeriod) {
            @Override
            long nextPermitWaitPeriodNanos() {
                return Long.MAX_VALUE;
            }
        };
    }

}
