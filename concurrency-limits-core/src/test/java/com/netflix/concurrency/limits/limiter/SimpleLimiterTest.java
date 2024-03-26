package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.FixedLimit;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

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

}
