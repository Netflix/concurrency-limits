package com.netflix.concurrency.limits.strategy;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class PercentageStrategyTest {
    @Test
    public void limitAllocatedToBins() {
        PercentageStrategy strategy = new PercentageStrategy(Arrays.asList(0.3, 0.7));
        strategy.setLimit(10);

        Assert.assertEquals(3, strategy.getBinLimit(0));
        Assert.assertEquals(7, strategy.getBinLimit(1));
    }
    
    @Test
    public void useExcessCapacityUntilTotalLimit() {
        PercentageStrategy strategy = new PercentageStrategy(Arrays.asList(0.3, 0.7));
        strategy.setLimit(10);
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(strategy.tryAcquire(0));
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
        }
        
        Assert.assertFalse(strategy.tryAcquire(0));
    }
    
    @Test
    public void exceedTotalLimitForUnusedBin() {
        PercentageStrategy strategy = new PercentageStrategy(Arrays.asList(0.3, 0.7));
        strategy.setLimit(10);
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(strategy.tryAcquire(0));
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
        }
        
        Assert.assertFalse(strategy.tryAcquire(0));
        
        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(strategy.tryAcquire(1));
            Assert.assertEquals(i+1, strategy.getBinBusyCount(1));
        }

        Assert.assertFalse(strategy.tryAcquire(1));
    }

    @Test
    public void rejectOnceAllLimitsReached() {
        PercentageStrategy strategy = new PercentageStrategy(Arrays.asList(0.3, 0.7));
        strategy.setLimit(10);
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(strategy.tryAcquire(0));
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
            Assert.assertEquals(i+1, strategy.getBusyCount());
        }
        
        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(strategy.tryAcquire(1));
            Assert.assertEquals(i+1, strategy.getBinBusyCount(1));
            Assert.assertEquals(i+4, strategy.getBusyCount());
        }

        Assert.assertFalse(strategy.tryAcquire(0));
        Assert.assertFalse(strategy.tryAcquire(1));
    }

    @Test
    public void releaseLimit() {
        PercentageStrategy strategy = new PercentageStrategy(Arrays.asList(0.3, 0.7));
        strategy.setLimit(10);
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(strategy.tryAcquire(0));
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
        }
    
        Assert.assertEquals(10, strategy.getBusyCount());
        
        Assert.assertFalse(strategy.tryAcquire(0));
        
        strategy.release(0);
        Assert.assertEquals(9, strategy.getBinBusyCount(0));
        Assert.assertEquals(9, strategy.getBusyCount());
        
        Assert.assertTrue(strategy.tryAcquire(0));
        Assert.assertEquals(10, strategy.getBinBusyCount(0));
        Assert.assertEquals(10, strategy.getBusyCount());
    }
    
    @Test
    public void setLimitReservesBusy() {
        PercentageStrategy strategy = new PercentageStrategy(Arrays.asList(0.3, 0.7));
        strategy.setLimit(10);
        Assert.assertEquals(3, strategy.getBinLimit(0));
        Assert.assertTrue(strategy.tryAcquire(0));
        Assert.assertEquals(1, strategy.getBinBusyCount(0));
        Assert.assertEquals(1, strategy.getBusyCount());
        
        strategy.setLimit(20);
        Assert.assertEquals(6, strategy.getBinLimit(0));
        Assert.assertEquals(1, strategy.getBinBusyCount(0));
        Assert.assertEquals(1, strategy.getBusyCount());
    }
}
