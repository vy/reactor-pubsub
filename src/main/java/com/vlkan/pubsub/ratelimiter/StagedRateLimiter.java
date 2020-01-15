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

package com.vlkan.pubsub.ratelimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An adaptive rate limiter with multiple success-failure rate limit stages. In
 * the absence of failure acknowledgements, excessive permit claims replace the
 * active stage with the next faster one, if there is any. Likewise, excessive
 * failure acknowledgements replace the active stage with the next slower one,
 * if there is any.
 *
 * <p>The stages are described in increasing success rate limit order using a
 * specification format as follows: <code>1/1m:, 1/30s:1/1m, 1/1s:2/1m,
 * :1/3m</code>. The specification is a comma-separated list of <i>[success
 * rate limit]:[failure rate limit]</i> pairs where, e.g., <code>2/1h</code> is
 * used to denote a rate limit of 2 permits per 1 hour. Temporal unit must be
 * one of h(ours), m(inutes), or s(econds). The initial failure rate limit and
 * the last success rate limit can be omitted to indicate no rate limits.) This
 * example will result in the following stages.
 *
 * <blockquote>
 * <table summary="Description of the rate limiter stages.">
 *     <thead>
 *         <tr>
 *             <th>stage</th>
 *             <th>success rate limit</th>
 *             <th>failure rate limit</th>
 *         </tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td>1</td>
 *             <td>1/1m (once per minute)</td>
 *             <td>infinite</td>
 *         </tr>
 *         <tr>
 *             <td>2</td>
 *             <td>1/30s (once per 30 second)</td>
 *             <td>1/1m (once per minute)</td>
 *         </tr>
 *         <tr>
 *             <td>3</td>
 *             <td>1/1s (once per second)</td>
 *             <td>2/1m (twice per minute)</td>
 *         </tr>
 *         <tr>
 *             <td>4</td>
 *             <td>infinute</td>
 *             <td>1/3m (once per 3 minute)</td>
 *         </tr>
 *     </tbody>
 * </table>
 * </blockquote>
 *
 * <p>By contract, initially the active stage is set to the one with the slowest
 * success rate limit.
 */
public class StagedRateLimiter {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagedRateLimiter.class);

    public static final String DEFAULT_RATE_LIMITER_SPEC = "1/1m:, 1/30s:1/1m, 1/1s:2/1m, :1/3m";

    private final String name;

    private final String spec;

    private final List<String> successRateLimitSpecs;

    private final List<String> failureRateLimitSpecs;

    private final List<RateLimiter> successRateLimiters;

    private final List<RateLimiter> failureRateLimiters;

    private final int rateLimiterCount;

    private volatile int activeRateLimiterIndex;

    @FunctionalInterface
    interface RateLimiterFactory {

        RateLimiter create(int maxPermitCountPerCycle, Duration cyclePeriod);

    }

    private StagedRateLimiter(
            String name,
            String spec,
            List<String> successRateLimitSpecs,
            List<String> failureRateLimitSpecs,
            List<RateLimiter> successRateLimiters,
            List<RateLimiter> failureRateLimiters) {
        this.name = name;
        this.spec = spec;
        this.successRateLimitSpecs = successRateLimitSpecs;
        this.failureRateLimitSpecs = failureRateLimitSpecs;
        this.successRateLimiters = successRateLimiters;
        this.failureRateLimiters = failureRateLimiters;
        this.rateLimiterCount = successRateLimiters.size();
        this.activeRateLimiterIndex = rateLimiterCount - 1;

    }

    static StagedRateLimiter of(String name, String spec) {
        return of(name, spec, RateLimiter::new);
    }

    static StagedRateLimiter of(String name, String spec, RateLimiterFactory rateLimiterFactory) {
        try {

            // Check spec pattern.
            String pattern =
                    // The first pair must just have a success spec.
                    "([0-9]+/[0-9]+[hms])?:" +
                            // Intermediate pairs must have a success spec and an optional failure spec.
                            "(\\s*,\\s*[0-9]+/[0-9]+[hms]:([0-9]+/[0-9]+[hms])?)+" +
                            // Both success and failure specs are optional for the last pair.
                            "\\s*,\\s*([0-9]+/[0-9]+[hms])?:([0-9]+/[0-9]+[hms])?";
            boolean patternMatched = spec.matches(pattern);
            if (!patternMatched) {
                String message = String.format("regex mismatch (pattern=%s)", pattern);
                throw new IllegalArgumentException(message);
            }

            // Parse spec.
            List<String> successRateLimitSpecs = new ArrayList<>();
            List<String> failureRateLimitSpecs = new ArrayList<>();
            List<RateLimiter> successRateLimiters = new ArrayList<>();
            List<RateLimiter> failureRateLimiters = new ArrayList<>();
            String[] pairSpecs = spec.split("\\s*,\\s*");
            for (String pairSpec : pairSpecs) {
                String[] singleSpecs = pairSpec.split("\\s*:\\s*", 2);
                @Nullable String successRateLimitSpec = singleSpecs[0].isEmpty() ? null : singleSpecs[0];
                @Nullable String failureRateLimitSpec = singleSpecs[1].isEmpty() ? null : singleSpecs[1];
                @Nullable RateLimiter successRateLimiter =
                        successRateLimitSpec != null
                                ? createRateLimiter(successRateLimitSpec, rateLimiterFactory)
                                : null;
                @Nullable RateLimiter failureRateLimiter =
                        failureRateLimitSpec != null
                                ? createRateLimiter(failureRateLimitSpec, rateLimiterFactory)
                                : null;
                successRateLimitSpecs.add(successRateLimitSpec);
                failureRateLimitSpecs.add(failureRateLimitSpec);
                successRateLimiters.add(successRateLimiter);
                failureRateLimiters.add(failureRateLimiter);
            }

            // Check if failure rate limits are lower than success ones.
            int rateLimiterCount = successRateLimiters.size();
            for (int rateLimiterIndex = 0; rateLimiterIndex < rateLimiterCount; rateLimiterIndex++) {
                @Nullable RateLimiter successRateLimiter = successRateLimiters.get(rateLimiterIndex);
                @Nullable RateLimiter failureRateLimiter = failureRateLimiters.get(rateLimiterIndex);
                if (successRateLimiter != null &&
                        failureRateLimiter != null &&
                        Double.compare(
                                successRateLimiter.getMaxPermitPerSecond(),
                                failureRateLimiter.getMaxPermitPerSecond()) <= 0) {
                    String successRateLimitSpec = successRateLimitSpecs.get(rateLimiterIndex);
                    String failureRateLimitSpec = failureRateLimitSpecs.get(rateLimiterIndex);
                    String message = String.format(
                            "was expecting failure rate limit to be lower than the one for success " +
                                    "(rateLimiterIndex=%d, successRateLimitSpec=%s, failureRateLimitSpec=%s)",
                            rateLimiterIndex, successRateLimitSpec, failureRateLimitSpec);
                    throw new IllegalArgumentException(message);
                }
            }

            // Check if success rate limiters are in ascending rate order.
            RateLimiter prevSuccessRateLimiter = successRateLimiters.get(0);
            for (int rateLimiterIndex = 1; rateLimiterIndex < rateLimiterCount; rateLimiterIndex++) {
                @Nullable RateLimiter nextSuccessRateLimiter = successRateLimiters.get(rateLimiterIndex);
                if (nextSuccessRateLimiter != null &&
                        Double.compare(
                                prevSuccessRateLimiter.getMaxPermitPerSecond(),
                                nextSuccessRateLimiter.getMaxPermitPerSecond()) >= 0) {
                    String prevSuccessRateLimitSpec = successRateLimitSpecs.get(rateLimiterIndex - 1);
                    String nextSuccessRateLimitSpec = successRateLimitSpecs.get(rateLimiterIndex);
                    String message = String.format(
                            "was expecting rate limiters in ascending rate order " +
                                    "(rateLimiterIndex=%d, prevSuccessRateLimitSpec=%s, nextSuccessRateLimitSpec=%s)",
                            rateLimiterIndex, prevSuccessRateLimitSpec, nextSuccessRateLimitSpec);
                    throw new IllegalArgumentException(message);
                }
            }

            // Create the instance.
            return new StagedRateLimiter(
                    name,
                    spec,
                    successRateLimitSpecs,
                    failureRateLimitSpecs,
                    successRateLimiters,
                    failureRateLimiters);

        } catch (Exception error) {
            String message = String.format("spec parse failure (name=%s, spec=%s)", name, spec);
            throw new RuntimeException(message, error);
        }

    }

    private static RateLimiter createRateLimiter(String spec, RateLimiterFactory rateLimiterFactory) {
        String pattern = "([0-9]+)/([0-9]+)([hms])";
        Matcher matcher = Pattern.compile(pattern).matcher(spec);
        boolean matched = matcher.matches();
        if (!matched) {
            String message = String.format("regex mismatch (pattern=%s, spec=%s)", pattern, spec);
            throw new IllegalArgumentException(message);
        }
        String maxPermitCountPerCycleText = matcher.group(1);
        String cyclePeriodAmountText = matcher.group(2);
        String cyclePeriodUnitText = matcher.group(3);
        int maxPermitCountPerCycle = Integer.parseInt(maxPermitCountPerCycleText);
        long cyclePeriodAmount = Long.parseLong(cyclePeriodAmountText);
        TemporalUnit cyclePeriodUnit = parseTemporalUnit(cyclePeriodUnitText);
        Duration cyclePeriod = Duration.of(cyclePeriodAmount, cyclePeriodUnit);
        return rateLimiterFactory.create(maxPermitCountPerCycle, cyclePeriod);
    }

    private static TemporalUnit parseTemporalUnit(String unit) {
        switch (unit) {
            case "h": return ChronoUnit.HOURS;
            case "m": return ChronoUnit.MINUTES;
            case "s": return ChronoUnit.SECONDS;
            default: throw new IllegalArgumentException("unknown temporal unit: " + unit);
        }
    }

    public String getName() {
        return name;
    }

    public String getSpec() {
        return spec;
    }

    synchronized String getActiveSuccessRateLimitSpec() {
        return successRateLimitSpecs.get(activeRateLimiterIndex);
    }

    synchronized String getActiveFailureRateLimitSpec() {
        return failureRateLimitSpecs.get(activeRateLimiterIndex);
    }

    /**
     * Claim a permit. Exceeding the current success rate limit will replace
     * the active stage with the next faster one, if there is any.
     *
     * @return Time in nanoseconds that the permit will become available. A
     *         zero value indicates that the permit is immediately available.
     */
    public synchronized long nextPermitWaitPeriodNanos() {
        @Nullable RateLimiter activeSuccessRateLimiter = successRateLimiters.get(activeRateLimiterIndex);
        if (activeSuccessRateLimiter != null) {
            long permitWaitPeriodNanos = activeSuccessRateLimiter.nextPermitWaitPeriodNanos();
            if (permitWaitPeriodNanos > 0) {
                int nextActiveRateLimiterIndex = activeRateLimiterIndex + 1;
                if (nextActiveRateLimiterIndex < rateLimiterCount) {
                    unsychronizedSetRate(nextActiveRateLimiterIndex);
                } else {
                    return permitWaitPeriodNanos;
                }
            }
        }
        return 0L;
    }

    /**
     * Acknowledge a failure. Exceeding the current failure rate limit will
     * replace the active stage with the next slower one, if there is any.
     */
    public synchronized void acknowledgeFailure() {
        @Nullable RateLimiter activeFailureRateLimiter = failureRateLimiters.get(activeRateLimiterIndex);
        boolean acquired = activeFailureRateLimiter != null && activeFailureRateLimiter.nextPermitWaitPeriodNanos() == 0L;
        if (!acquired) {
            int nextActiveRateLimiterIndex = activeRateLimiterIndex - 1;
            if (nextActiveRateLimiterIndex >= 0) {
                unsychronizedSetRate(nextActiveRateLimiterIndex);
            }
        }
    }

    private void unsychronizedSetRate(int nextActiveRateLimiterIndex) {
        String direction = activeRateLimiterIndex < nextActiveRateLimiterIndex ? "up" : "down";
        activeRateLimiterIndex = nextActiveRateLimiterIndex;
        if (LOGGER.isInfoEnabled()) {
            String successRateLimitSpec = successRateLimitSpecs.get(nextActiveRateLimiterIndex);
            String failureRateLimitSpec = failureRateLimitSpecs.get(nextActiveRateLimiterIndex);
            LOGGER.info(
                    "adjusting rate (name={}, direction={}, successRateLimitSpec={}, failureRateLimitSpec={})",
                    name, direction, successRateLimitSpec, failureRateLimitSpec);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String name;

        private String spec = DEFAULT_RATE_LIMITER_SPEC;

        private Builder() {}

        public Builder setName(String name) {
            this.name = Objects.requireNonNull(name, "name");
            return this;
        }

        public Builder setSpec(String spec) {
            this.spec = Objects.requireNonNull(spec, "spec");
            return this;
        }

        public StagedRateLimiter build() {
            Objects.requireNonNull(name, "name");
            return of(name, spec, RateLimiter::new);
        }

    }

}
