package com.netflix.concurrency.limits.strategy;

import java.util.Optional;

import org.junit.Test;

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
        Assert.assertTrue(strategy.tryAcquire(null).isPresent());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void exceedingLimitReturnsFalse() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Assert.assertTrue(strategy.tryAcquire(null).isPresent());
        Assert.assertFalse(strategy.tryAcquire(null).isPresent());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void acquireAndRelease() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Optional<Runnable> completion = strategy.tryAcquire(null);
        Assert.assertTrue(completion.isPresent());
        Assert.assertEquals(1, strategy.getBusyCount());
        
        completion.get().run();
        
        Assert.assertEquals(0, strategy.getBusyCount());

        Assert.assertTrue(strategy.tryAcquire(null).isPresent());
        Assert.assertEquals(1, strategy.getBusyCount());
    }
}
