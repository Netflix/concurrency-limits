package com.netflix.concurrency.limits.strategy;

import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import com.netflix.concurrency.limits.Strategy.Token;

import junit.framework.Assert;

public class SimpleStrategyTest {
    @Test
    public void limitLessThanZeroSetAs1() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        
        strategy.setLimit(-10);
        Assert.assertEquals(1, strategy.getLimit());
    }
    
    @Test
    public void initialState() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Assert.assertEquals(1, strategy.getLimit());
        Assert.assertEquals(0, strategy.getBusyCount());
    }
    
    @Test
    public void acquireIncrementsBusy() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Assert.assertEquals(0, strategy.getBusyCount());
        Assert.assertTrue(strategy.tryAcquire(null).isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void exceedingLimitReturnsFalse() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Assert.assertTrue(strategy.tryAcquire(null).isAcquired());
        Assert.assertFalse(strategy.tryAcquire(null).isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void acquireAndRelease() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Token completion = strategy.tryAcquire(null);
        Assert.assertTrue(completion.isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
        
        completion.release();
        
        Assert.assertEquals(0, strategy.getBusyCount());

        Assert.assertTrue(strategy.tryAcquire(null).isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void tryAcquireWithMultipleThreads() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        Runnable acquireBarrier = () -> {
            latch.countDown();
            try {
                Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        SimpleStrategy<Void> strategy = new SimpleStrategy<>(
            EmptyMetricRegistry.INSTANCE,
            acquireBarrier);
        strategy.setLimit(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        Future<Token> future1 = executor.submit(() -> strategy.tryAcquire(null));
        Future<Token> future2 = executor.submit(() -> strategy.tryAcquire(null));

        // Assert only 1 thread acquired a token.
        Assert.assertTrue(future1.get().isAcquired() != future2.get().isAcquired());
    }
}
