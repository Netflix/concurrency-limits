package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.SettableLimit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LifoBlockingLimiterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LifoBlockingLimiterTest.class);

    final ExecutorService executor = Executors.newCachedThreadPool();

    final SettableLimit limit = SettableLimit.startingAt(4);

    final SimpleLimiter<Void> simpleLimiter = SimpleLimiter
            .newBuilder()
            .limit(limit)
            .build();

    final LifoBlockingLimiter<Void> blockingLimiter = LifoBlockingLimiter.newBuilder(simpleLimiter)
            .backlogSize(10)
            .backlogTimeout(1, TimeUnit.SECONDS)
            .build();

    @Test
    public void blockWhenFullAndTimeout() {
        // Acquire all 4 available tokens
        for (int i = 0; i < 4; i++) {
            Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
            Assert.assertTrue(listener.isPresent());
        }

        // Next acquire will block for 1 second
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        Assert.assertTrue(duration >= 1);
        Assert.assertFalse(listener.isPresent());
    }

    @Test
    public void unblockWhenFullBeforeTimeout() {
        // Acquire all 4 available tokens
        List<Optional<Limiter.Listener>> listeners = acquireN(blockingLimiter, 4);

        // Schedule one to release in 250 msec
        Executors.newSingleThreadScheduledExecutor().schedule(() -> listeners.get(0).get().onSuccess(), 250, TimeUnit.MILLISECONDS);

        // Next acquire will block for 1 second
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("Duration = " + duration, duration >= 200);
        Assert.assertTrue(listener.isPresent());
    }

    @Test
    public void rejectWhenBacklogSizeReached() throws InterruptedException {
        acquireNAsync(blockingLimiter, 14);

        // Small delay to make sure all acquire() calls have been made
        TimeUnit.MILLISECONDS.sleep(250);

        // Next acquire will reject with no delay
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("Duration = " + duration, duration < 100);
        Assert.assertFalse(listener.isPresent());
    }

    @Test
    public void adaptWhenLimitIncreases() {
        acquireN(blockingLimiter, 4);

        limit.setLimit(5);

        // Next acquire will succeed with no delay
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("Duration = " + duration, duration < 100);
        Assert.assertTrue(listener.isPresent());
    }

    @Test
    public void adaptWhenLimitDecreases() {
        List<Optional<Limiter.Listener>> listeners = acquireN(blockingLimiter, 4);

        limit.setLimit(3);

        listeners.get(0).get().onSuccess();

        // Next acquire will reject and block
        long start = System.nanoTime();
        Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
        long duration = TimeUnit.SECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("Duration = " + duration, duration >= 1);
        Assert.assertFalse(listener.isPresent());
    }

    @Test
    public void verifyFifoOrder() {
        // Make sure all tokens are acquired
        List<Optional<Limiter.Listener>> firstBatch = acquireN(blockingLimiter, 4);

        // Kick off 5 requests with a small delay to ensure futures are created in the correct order
        List<Integer> values = new CopyOnWriteArrayList<>();
        List<CompletableFuture<Void>> futures = IntStream.range(0, 5)
                .peek(i -> {
                    try {
                        TimeUnit.MILLISECONDS.sleep(50);
                    } catch (InterruptedException e) {
                    }
                })
                .mapToObj(i -> CompletableFuture.<Void>supplyAsync(() -> {
                    Optional<Limiter.Listener> listener = blockingLimiter.acquire(null);
                    if (!listener.isPresent()) {
                        values.add(-1);
                    }
                    try {
                        values.add(i);
                    } finally {
                        listener.get().onSuccess();
                    }
                    return null;
                }, executor))
                .collect(Collectors.toList());

        // Release the first batch of tokens
        firstBatch.forEach(listener -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
            }
            listener.get().onSuccess();
        });

        // Make sure all requests finished
        futures.forEach(future -> {
                    try {
                        future.get();
                    } catch (Exception e) {
                    }
                });

        // Verify that results are in reverse order
        Assert.assertEquals(Arrays.asList(4, 3, 2, 1, 0), values);
    }

    // this test reproduces the condition where a thread acquires a token just as it is timing out.
    // before that was fixed, it would lead to a token getting lost.
    @Test
    public void timeoutAcquireRaceCondition() throws InterruptedException, ExecutionException {
        // a limiter with a short timeout, and large backlog (we don't want it to hit that limit)
        LifoBlockingLimiter<Void> limiter = LifoBlockingLimiter.newBuilder(simpleLimiter)
            .backlogSize(1000)
            .backlogTimeout(10, TimeUnit.MILLISECONDS)
            .build();

        // acquire all except one token
        acquireN(limiter, 3);

        // try to reproduce the problem a couple of times
        for (int round = 0; round < 10; round++) {
            // indicates if there has already been a timeout
            AtomicBoolean firstTimeout = new AtomicBoolean(false);
            // take the last token
            Limiter.Listener one = limiter.acquire(null).get();
            // in a bunch of threads in parallel, try to take one more. all of these will start to
            // time out at around the same time
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                futures.add(executor.submit(() -> {
                    Optional<Limiter.Listener> listener = limiter.acquire(null);
                    if (listener.isPresent()) {
                        // if we got the last token, release it again. this might give it to a
                        // thread that is in the process of timing out
                        listener.get().onSuccess();
                    } else if (firstTimeout.compareAndSet(false, true)) {
                        // if this is the first one that times out, then other threads are going to
                        // start timing out soon too, so it's time to release a token
                        one.onSuccess();
                    }
                    return null;
                }));
            }
            // wait for this round to finish
            for (Future<?> future : futures) {
                future.get();
            }
            Assert.assertEquals(3, simpleLimiter.getInflight());
        }
    }

    private List<Optional<Limiter.Listener>> acquireN(Limiter<Void> limiter, int N) {
        return IntStream.range(0, N)
                .mapToObj(i -> limiter.acquire(null))
                .peek(listener -> Assert.assertTrue(listener.isPresent()))
                .collect(Collectors.toList());
    }

    private List<CompletableFuture<Optional<Limiter.Listener>>> acquireNAsync(Limiter<Void> limiter, int N) {
        return IntStream.range(0, N)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> limiter.acquire(null), executor))
                .collect(Collectors.toList());
    }
}
