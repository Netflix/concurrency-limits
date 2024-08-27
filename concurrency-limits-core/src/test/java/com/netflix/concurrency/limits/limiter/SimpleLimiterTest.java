package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiterTest.TestPartitionedLimiter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleLimiterTest {

    @Test
    public void useLimiterCapacityUntilTotalLimit() {
        SimpleLimiter<String> limiter = SimpleLimiter.newBuilder()
                .limit(FixedLimit.of(10))
                .build();

        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
        }

        // Rejected call after total limit is utilized
        Assert.assertFalse(limiter.acquire("live").isPresent());
        Assert.assertEquals(10, limiter.getInflight());
    }

    @Test
    public void testReleaseLimit() {
        SimpleLimiter<String> limiter = SimpleLimiter.newBuilder()
                .limit(FixedLimit.of(10))
                .build();

        Optional<Limiter.Listener> completion = limiter.acquire("live");
        for (int i = 1; i < 10; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
        }

        Assert.assertEquals(10, limiter.getInflight());
        Assert.assertFalse(limiter.acquire("live").isPresent());

        // Release token
        completion.get().onSuccess();
        Assert.assertEquals(9, limiter.getInflight());

        Assert.assertTrue(limiter.acquire("live").isPresent());
        Assert.assertEquals(10, limiter.getInflight());
    }

    @Test
    public void testSimpleBypassLimiter() {
        SimpleLimiter<String> limiter = SimpleLimiter.<String>newBuilder()
                .limit(FixedLimit.of(10))
                .bypassLimitResolverInternal((context) -> context.equals("admin"))
                .build();

        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
            Assert.assertEquals(i+1, limiter.getInflight());
        }

        // Verify calls with passing bypass condition will return a token
        // whereas remaining calls will be throttled since inflight count is greater than the limit
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(limiter.acquire("live").isPresent());
            Assert.assertTrue(limiter.acquire("admin").isPresent());
        }
    }

    @Test
    public void testSimpleBypassLimiterDefault() {
        SimpleLimiter<String> limiter = SimpleLimiter.<String>newBuilder()
                .limit(FixedLimit.of(10))
                .build();

        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
            Assert.assertEquals(i+1, limiter.getInflight());
        }

        // Verify that no calls are bypassed by default
        Assert.assertFalse(limiter.acquire("live").isPresent());
        Assert.assertFalse(limiter.acquire("admin").isPresent());
    }

    @Test
    public void testConcurrentSimple() throws InterruptedException {
        final int THREAD_COUNT = 100;
        final int ITERATIONS = 1000;
        final int LIMIT = 10;

        SimpleLimiter<String> limiter = (SimpleLimiter<String>) TestPartitionedLimiter.newBuilder()
                .limit(FixedLimit.of(LIMIT))
                .partition("default", 1.0)
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger rejectionCount = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < ITERATIONS; j++) {
                        Optional<Listener> listener = limiter.acquire("default");
                        if (listener.isPresent()) {
                            try {
                                int current = limiter.getInflight();
                                maxConcurrent.updateAndGet(max -> Math.max(max, current));
                                successCount.incrementAndGet();
                                Thread.sleep(1); // Simulate some work
                            } finally {
                                listener.get().onSuccess();
                            }
                        } else {
                            rejectionCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("Success count: " + successCount.get());
        System.out.println("Rejection count: " + rejectionCount.get());
        System.out.println("Max concurrent: " + maxConcurrent.get());

        Assert.assertTrue("Max concurrent should not exceed limit", maxConcurrent.get() <= LIMIT);
        Assert.assertEquals("Total attempts should equal success + rejections", 
                            THREAD_COUNT * ITERATIONS, successCount.get() + rejectionCount.get());
    }

}
