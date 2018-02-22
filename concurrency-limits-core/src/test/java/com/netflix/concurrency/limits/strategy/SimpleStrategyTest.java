package com.netflix.concurrency.limits.strategy;

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
}
