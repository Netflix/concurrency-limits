package com.netflix.concurrency.limits.strategy;

import org.junit.Test;

import junit.framework.Assert;

public class SimpleStrategyTest {
    @Test
    public void limitLessThanZeroSetAs1() {
        SimpleStrategy strategy = new SimpleStrategy();
        
        strategy.setLimit(-10);
        Assert.assertEquals(1, strategy.getLimit());
    }
    
    @Test
    public void initialState() {
        SimpleStrategy strategy = new SimpleStrategy();
        Assert.assertEquals(1, strategy.getLimit());
        Assert.assertEquals(0, strategy.getBusyCount());
    }
    
    @Test
    public void acquireIncrementsBusy() {
        SimpleStrategy strategy = new SimpleStrategy();
        Assert.assertEquals(0, strategy.getBusyCount());
        Assert.assertTrue(strategy.tryAcquire(null));
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void exceedingLimitReturnsFalse() {
        SimpleStrategy strategy = new SimpleStrategy();
        Assert.assertTrue(strategy.tryAcquire(null));
        Assert.assertFalse(strategy.tryAcquire(null));
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void acquireAndRelease() {
        SimpleStrategy strategy = new SimpleStrategy();
        Assert.assertTrue(strategy.tryAcquire(null));
        Assert.assertEquals(1, strategy.getBusyCount());
        
        strategy.release(null);
        Assert.assertEquals(0, strategy.getBusyCount());

        Assert.assertTrue(strategy.tryAcquire(null));
        Assert.assertEquals(1, strategy.getBusyCount());
    }
}
