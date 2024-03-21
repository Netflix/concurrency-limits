package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.SettableLimit;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class AbstractPartitionedLimiterTest {
    public static class TestPartitionedLimiter extends AbstractPartitionedLimiter<String> {
        public static class Builder extends AbstractPartitionedLimiter.Builder<Builder, String> {
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
                .bypassLimitResolver(new ShouldBypassPredicate())
                .build();

        for (int i = 0; i < 1; i++) {
            Assert.assertTrue(limiter.acquire("batch").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("batch").getInflight());
            Assert.assertTrue(limiter.acquire("admin").isPresent());
        }

        for (int i = 0; i < 9; i++) {
            Assert.assertTrue(limiter.acquire("live").isPresent());
            Assert.assertEquals(i+1, limiter.getPartition("live").getInflight());
            Assert.assertTrue(limiter.acquire("admin").isPresent());
        }
    }

    @Test
    public void testBypassSimpleLimiter() {

        SimpleLimiter<String> limiter = (SimpleLimiter<String>) TestPartitionedLimiter.newBuilder()
                .limit(FixedLimit.of(10))
                .bypassLimitResolver(new ShouldBypassPredicate())
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
}
