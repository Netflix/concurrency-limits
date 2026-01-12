package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.SettableLimit;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

public class AbstractPartitionedLimiterTest {
    public static class TestPartitionedLimiter extends AbstractPartitionedLimiter<String> {
        public static class Builder extends AbstractPartitionedLimiter.Builder<Builder, String> {

            public Builder() {
                super("test");
            }

            @Override
            protected Builder self() {
                return this;
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public TestPartitionedLimiter(Builder builder) {
            super(builder);
        }
    }

    public static class ShouldBypassPredicate implements Predicate<String> {
        @Override
        public boolean test(String s) {
            return s.contains("admin");
        }
    }

    @Test
    public void limitAllocatedToBins() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .partitionResolver(Function.identity())
                .partition("batch", 0.3)
                .partition("live", 0.7)
                .limit(FixedLimit.of(10))
                .build();

        Assert.assertEquals(3, limiter.getPartition("batch").getLimit());
        Assert.assertEquals(7, limiter.getPartition("live").getLimit());
    }

    @Test
    public void useExcessCapacityUntilTotalLimit() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .partitionResolver(Function.identity())
                .partition("batch", 0.3)
                .partition("live", 0.7)
                .limit(FixedLimit.of(10))
                .build();

        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(limiter.acquire("batch").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("batch").getInflight());
        }

        Assert.assertFalse(limiter.acquire("batch").isPresent());
    }

    @Test
    public void exceedTotalLimitForUnusedBin() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .partitionResolver(Function.identity())
                .partition("batch", 0.3)
                .partition("live", 0.7)
                .limit(FixedLimit.of(10))
                .build();

        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(limiter.acquire("batch").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("batch").getInflight());
        }

        Assert.assertFalse(limiter.acquire("batch").isPresent());

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("live").getInflight());
        }

        Assert.assertFalse(limiter.acquire("live").isPresent());
    }

    @Test
    public void rejectOnceAllLimitsReached() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .partitionResolver(Function.identity())
                .partition("batch", 0.3)
                .partition("live", 0.7)
                .limit(FixedLimit.of(10))
                .build();

        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(limiter.acquire("batch").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("batch").getInflight());
            Assert.assertEquals(i+1, limiter.getInflight());
        }

        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("live").getInflight());
            Assert.assertEquals(i+4, limiter.getInflight());
        }

        Assert.assertFalse(limiter.acquire("batch").isPresent());
        Assert.assertFalse(limiter.acquire("live").isPresent());
    }

    @Test
    public void releaseLimit() {
        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .partitionResolver(Function.identity())
                .partition("batch", 0.3)
                .partition("live", 0.7)
                .limit(FixedLimit.of(10))
                .build();

        Optional<Limiter.Listener> completion = limiter.acquire("batch");
        for (int i = 1; i < 10; i++) {
            Assert.assertTrue(limiter.acquire("batch").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("batch").getInflight());
        }

        Assert.assertEquals(10, limiter.getInflight());

        Assert.assertFalse(limiter.acquire("batch").isPresent());

        completion.get().onSuccess();
        Assert.assertEquals(9, limiter.getPartition("batch").getInflight());
        Assert.assertEquals(9, limiter.getInflight());

        Assert.assertTrue(limiter.acquire("batch").isPresent());
        Assert.assertEquals(10, limiter.getPartition("batch").getInflight());
        Assert.assertEquals(10, limiter.getInflight());
    }

    @Test
    public void setLimitReservesBusy() {
        SettableLimit limit = SettableLimit.startingAt(10);

        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .partitionResolver(Function.identity())
                .partition("batch", 0.3)
                .partition("live", 0.7)
                .limit(limit)
                .build();

        limit.setLimit(10);
        Assert.assertEquals(3, limiter.getPartition("batch").getLimit());
        Assert.assertTrue(limiter.acquire("batch").isPresent());
        Assert.assertEquals(1, limiter.getPartition("batch").getInflight());
        Assert.assertEquals(1, limiter.getInflight());

        limit.setLimit(20);
        Assert.assertEquals(6, limiter.getPartition("batch").getLimit());
        Assert.assertEquals(1, limiter.getPartition("batch").getInflight());
        Assert.assertEquals(1, limiter.getInflight());
    }

    @Test
    public void testBypassPartitionedLimiter() {

        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .partitionResolver(Function.identity())
                .partition("batch", 0.1)
                .partition("live", 0.9)
                .limit(FixedLimit.of(10))
                .bypassLimitResolverInternal(new ShouldBypassPredicate())
                .build();

        Assert.assertTrue(limiter.acquire("batch").isPresent());
        Assert.assertEquals(1, limiter.getPartition("batch").getInflight());
        Assert.assertTrue(limiter.acquire("admin").isPresent());

        for (int i = 0; i < 9; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("live").getInflight());
            Assert.assertTrue(limiter.acquire("admin").isPresent());
        }

        // Verify that bypassed requests are able to proceed even when the limiter is full
        Assert.assertFalse(limiter.acquire("batch").isPresent());
        Assert.assertEquals(1, limiter.getPartition("batch").getInflight());
        Assert.assertFalse(limiter.acquire("live").isPresent());
        Assert.assertEquals(9, limiter.getPartition("live").getInflight());
        Assert.assertEquals(10, limiter.getInflight());
        Assert.assertTrue(limiter.acquire("admin").isPresent());
    }

    @Test
    public void testBypassSimpleLimiter() {

        SimpleLimiter<String> limiter = (SimpleLimiter<String>) TestPartitionedLimiter.newBuilder()
                .limit(FixedLimit.of(10))
                .bypassLimitResolverInternal(new ShouldBypassPredicate())
                .build();

        int inflightCount = 0;
        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(limiter.acquire("request").isPresent());
            Assert.assertEquals(i+1, limiter.getInflight());
            inflightCount++;
        }

        for (int i = 0; i < 15; i++) {
            Assert.assertTrue(limiter.acquire("admin").isPresent());
            Assert.assertEquals(inflightCount, limiter.getInflight());
        }

        for (int i = 0; i < 5; i++) {
            Assert.assertTrue(limiter.acquire("request").isPresent());
            Assert.assertEquals(inflightCount+i+1, limiter.getInflight());
        }

        // Calls with passing bypass condition will return a token
        // whereas remaining calls will be throttled since inflight count is greater than the limit
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(limiter.acquire("request").isPresent());
            Assert.assertTrue(limiter.acquire("admin").isPresent());
        }
    }

    @Test
    public void testConcurrentPartitions() throws InterruptedException {
        final int THREAD_COUNT = 5;
        final int ITERATIONS = 500;
        final int LIMIT = 20;

        AbstractPartitionedLimiter<String> limiter = (AbstractPartitionedLimiter<String>) TestPartitionedLimiter.newBuilder()
                .limit(FixedLimit.of(LIMIT))
                .partitionResolver(Function.identity())
                .partition("A", 0.5)
                .partition("B", 0.3)
                .partition("C", 0.2)
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT * 3);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT * 3);
        Map<String, AtomicInteger> successCounts = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> rejectionCounts = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> maxConcurrents = new ConcurrentHashMap<>();
        AtomicInteger globalMaxInflight = new AtomicInteger(0);

        for (String partition : Arrays.asList("A", "B", "C")) {
            successCounts.put(partition, new AtomicInteger(0));
            rejectionCounts.put(partition, new AtomicInteger(0));
            maxConcurrents.put(partition, new AtomicInteger(0));

            for (int i = 0; i < THREAD_COUNT; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < ITERATIONS; j++) {
                            Optional<Listener> listener = limiter.acquire(partition);
                            if (listener.isPresent()) {
                                try {
                                    int current = limiter.getPartition(partition).getInflight();
                                    maxConcurrents.get(partition).updateAndGet(max -> Math.max(max, current));
                                    successCounts.get(partition).incrementAndGet();
                                    globalMaxInflight.updateAndGet(max -> Math.max(max, limiter.getInflight()));
                                    Thread.sleep(1); // Simulate some work
                                } finally {
                                    listener.get().onSuccess();
                                }
                            } else {
                                rejectionCounts.get(partition).incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endLatch.countDown();
                    }
                });
            }
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        StringBuilder resultSummary = new StringBuilder();
        for (String partition : Arrays.asList("A", "B", "C")) {
            int successCount = successCounts.get(partition).get();
            int rejectionCount = rejectionCounts.get(partition).get();
            int maxConcurrent = maxConcurrents.get(partition).get();

            resultSummary.append(String.format("%s(success=%d,reject=%d,maxConcurrent=%d) ",
                                 partition, successCount, rejectionCount, maxConcurrent));

            Assert.assertTrue("Max concurrent for " + partition + " should not exceed global limit. " + resultSummary,
                    maxConcurrent <= LIMIT);
            Assert.assertEquals("Total attempts for " + partition + " should equal success + rejections. " + resultSummary,
                                THREAD_COUNT * ITERATIONS,
                                successCount + rejectionCount);
        }

        Assert.assertTrue("Global max inflight should not exceed total limit. " + resultSummary,
                          globalMaxInflight.get() <= LIMIT + THREAD_COUNT);
    }

}
