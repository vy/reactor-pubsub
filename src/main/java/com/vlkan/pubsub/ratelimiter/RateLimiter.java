/*
 * Copyright 2016-2019 Robert Winkler and Bohdan Storozhuk
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

package com.vlkan.pubsub.ratelimiter;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.UnaryOperator;

/**
 * A thread-safe rate limiter allowing claim of {@link #maxPermitCountPerCycle}
 * permits during cycles of {@link #cyclePeriodNanos} duration.
 * <p>
 * This class is a (shamelessly) trimmed down version of
 * <a href="https://github.com/resilience4j/resilience4j/blob/master/resilience4j-ratelimiter/src/main/java/io/github/resilience4j/ratelimiter/internal/AtomicRateLimiter.java">AtomicRateLimiter</a>.
 */
class RateLimiter {

    private static final long CLASS_INIT_TIME_NANOS = System.nanoTime();

    private final int maxPermitCountPerCycle;

    private final Duration cyclePeriod;

    private final long cyclePeriodNanos;

    private final AtomicReference<State> state;

    private static final class State {

        private final long cycleIndex;

        private final int permitCount;

        private final long permitWaitPeriodNanos;

        private State(long cycleIndex, int permitCount, long permitWaitPeriodNanos) {
            this.cycleIndex = cycleIndex;
            this.permitCount = permitCount;
            this.permitWaitPeriodNanos = permitWaitPeriodNanos;
        }

    }

    RateLimiter(int maxPermitCountPerCycle, Duration cyclePeriod) {
        this.maxPermitCountPerCycle = maxPermitCountPerCycle;
        this.cyclePeriod = cyclePeriod;
        this.cyclePeriodNanos = cyclePeriod.toNanos();
        this.state = new AtomicReference<>(new State(0, maxPermitCountPerCycle, 0));
    }

    double getMaxPermitPerSecond() {
        return (1e9 * maxPermitCountPerCycle) / cyclePeriodNanos;
    }

    /**
     * Returns an estimate of the acquire permits per second.
     */
    public double getAcquiredPermitCountPerSecond() {
        State currentState = state.get();
        State estimatedState = calculateNextState(currentState);
        return (1e9 * (maxPermitCountPerCycle - estimatedState.permitCount)) / cyclePeriodNanos;
    }

    /**
     * Claim a permit.
     *
     * @return Time in nanoseconds that the permit will become available. A
     *         zero value indicates that the permit is immediately available.
     */
    long nextPermitWaitPeriodNanos() {
        State modifiedState = updateState();
        return modifiedState.permitWaitPeriodNanos;
    }

    /**
     * {@link AtomicReference#updateAndGet(UnaryOperator)} clone employing
     * {@link #compareAndSetWithBackOff(State, State)} rather than
     * {@link AtomicReference#compareAndSet(Object, Object)}.
     */
    private State updateState() {
        RateLimiter.State prev;
        RateLimiter.State next;
        do {
            prev = state.get();
            next = calculateNextState(prev);
        } while (!compareAndSetWithBackOff(prev, next));
        return next;
    }

    /**
     * {@link AtomicReference#compareAndSet(Object, Object)} shortcut with a
     * constant back off. This technique was originally described in
     * <a href="https://arxiv.org/abs/1305.5800">Lightweight Contention
     * Management for Efficient Compare-and-Swap Operations</a> and showed
     * great results in benchmarks.
     */
    private boolean compareAndSetWithBackOff(State current, State next) {
        if (state.compareAndSet(current, next)) {
            return true;
        }
        LockSupport.parkNanos(1); // back-off
        return false;
    }

    /**
     * A side-effect-free function that can calculate the next {@link State}
     * from the current. It determines time duration that you should wait for
     * permit.
     */
    private State calculateNextState(State activeState) {

        // Determine the current time and cycle.
        long currentTimeNanos = System.nanoTime() - CLASS_INIT_TIME_NANOS;
        long currentCycleIndex = currentTimeNanos / cyclePeriodNanos;

        // Determine the next cycle and permit count.
        long nextCycleIndex = activeState.cycleIndex;
        int nextPermitCount = activeState.permitCount;
        if (nextCycleIndex != currentCycleIndex) {
            long elapsedCycleCount = currentCycleIndex - nextCycleIndex;
            long accumulatedPermitCount = elapsedCycleCount * maxPermitCountPerCycle;
            nextCycleIndex = currentCycleIndex;
            nextPermitCount = Math.toIntExact(Long.min(
                    nextPermitCount + accumulatedPermitCount,
                    maxPermitCountPerCycle));
        }

        // Determine the next wait period.
        long nextPermitWaitPeriodNanos = calculatePermitWaitPeriodNanos(
                nextPermitCount, currentTimeNanos, currentCycleIndex);

        // Instantiate the next state.
        boolean permitAllowed = nextPermitWaitPeriodNanos <= 0;
        return permitAllowed
                ? new State(nextCycleIndex, nextPermitCount - 1, nextPermitWaitPeriodNanos)
                : new State(nextCycleIndex, nextPermitCount, nextPermitWaitPeriodNanos);

    }

    /**
     * Calculates the time for the next permit to become available. That is,
     * [time to the next cycle] + [duration of full cycles until acquired
     * permits expire].
     *
     * @param permitCount       currently available permits
     * @param currentTimeNanos  current time in nanoseconds
     * @param currentCycleIndex current {@link RateLimiter} cycle
     * @return nanoseconds to wait for the next permit
     */
    private long calculatePermitWaitPeriodNanos(
            int permitCount,
            long currentTimeNanos,
            long currentCycleIndex) {
        if (permitCount > 0) {
            return 0L;
        }
        long nextCycleTimeNanos = (currentCycleIndex + 1) * cyclePeriodNanos;
        long nextCycleCompletionPeriodNanos = nextCycleTimeNanos - currentTimeNanos;
        int pendingFullCycleCount = (-permitCount) / maxPermitCountPerCycle;
        return (pendingFullCycleCount * cyclePeriodNanos) + nextCycleCompletionPeriodNanos;
    }

    @Override
    public String toString() {
        return String.format("%d/%s", maxPermitCountPerCycle, cyclePeriod);
    }

}
