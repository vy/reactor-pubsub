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

package com.vlkan.pubsub.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * A {@link ScheduledThreadPoolExecutor} wrapper enforcing a bound on the task
 * queue size. Excessive task queue growth yields {@link
 * RejectedExecutionException} errors. {@link RejectedExecutionHandler}s are
 * not supported since they expect a {@link ThreadPoolExecutor} in their
 * arguments.
 *
 * <p>Java Standard library unfortunately doesn't provide any {@link
 * ScheduledExecutorService} implementations that one can provide a bound on
 * the task queue. This shortcoming is prone to hide backpressure problems. See
 * <a href="http://cs.oswego.edu/pipermail/concurrency-interest/2019-April/016861.html">the
 * relevant concurrency-interest discussion</a> for {@link java.util.concurrent}
 * lead Doug Lea's tip for enforcing a bound via {@link
 * ScheduledThreadPoolExecutor#getQueue()}.
 */
@SuppressWarnings("NullableProblems")
public class BoundedScheduledThreadPoolExecutor implements ScheduledExecutorService {

    private final int queueCapacity;

    private final ScheduledThreadPoolExecutor executor;

    public BoundedScheduledThreadPoolExecutor(
            int queueCapacity,
            ScheduledThreadPoolExecutor executor) {
        if (queueCapacity < 1) {
            throw new IllegalArgumentException(
                    "was expecting a non-zero positive queue capacity");
        }
        this.queueCapacity = queueCapacity;
        this.executor = executor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized ScheduledFuture<?> schedule(
            Runnable command,
            long delay,
            TimeUnit unit) {
        ensureQueueCapacity(1);
        return executor.schedule(command, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized <V> ScheduledFuture<V> schedule(
            Callable<V> callable,
            long delay,
            TimeUnit unit) {
        ensureQueueCapacity(1);
        return executor.schedule(callable, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command,
            long initialDelay,
            long period,
            TimeUnit unit) {
        ensureQueueCapacity(1);
        return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command,
            long initialDelay,
            long delay,
            TimeUnit unit) {
        ensureQueueCapacity(1);
        return executor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        executor.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTerminated() {
        return executor.isTerminated();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized  <T> Future<T> submit(Callable<T> task) {
        ensureQueueCapacity(1);
        return executor.submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized <T> Future<T> submit(Runnable task, T result) {
        ensureQueueCapacity(1);
        return executor.submit(task, result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Future<?> submit(Runnable task) {
        ensureQueueCapacity(1);
        return executor.submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        ensureQueueCapacity(tasks.size());
        return executor.invokeAll(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit)
            throws InterruptedException {
        ensureQueueCapacity(tasks.size());
        return executor.invokeAll(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
        ensureQueueCapacity(tasks.size());
        return executor.invokeAny(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized <T> T invokeAny(
            Collection<? extends Callable<T>> tasks,
            long timeout,
            TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        ensureQueueCapacity(tasks.size());
        return executor.invokeAny(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void execute(Runnable command) {
        ensureQueueCapacity(1);
        executor.submit(command);
    }

    private void ensureQueueCapacity(int taskCount) {
        int queueSize = executor.getQueue().size();
        if ((queueSize + taskCount) >= queueCapacity) {
            throw new RejectedExecutionException();
        }
    }

}
