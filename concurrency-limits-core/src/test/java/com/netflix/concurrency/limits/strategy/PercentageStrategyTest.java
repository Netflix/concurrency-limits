package com.netflix.concurrency.limits.strategy;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

public class PercentageStrategyTest {
    @Test
    public void limitAllocatedToBins() {
        PredicatePartitionStrategy<String> strategy = PredicatePartitionStrategy.<String>newBuilder()
                .add("batch", 0.3, str -> str.equals("batch"))
                .add("live", 0.7, str -> str.equals("live"))
                .build();
        strategy.setLimit(10);

        Assert.assertEquals(3, strategy.getBinLimit(0));
        Assert.assertEquals(7, strategy.getBinLimit(1));
    }
    
    @Test
    public void useExcessCapacityUntilTotalLimit() {
        PredicatePartitionStrategy<String> strategy = PredicatePartitionStrategy.<String>newBuilder()
                .add("batch", 0.3, str -> str.equals("batch"))
                .add("live", 0.7, str -> str.equals("live"))
                .build();
        strategy.setLimit(10);
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(strategy.tryAcquire("batch").isPresent());
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
        }
        
        Assert.assertFalse(strategy.tryAcquire("batch").isPresent());
    }
    
    @Test
    public void exceedTotalLimitForUnusedBin() {
        PredicatePartitionStrategy<String> strategy = PredicatePartitionStrategy.<String>newBuilder()
                .add("batch", 0.3, str -> str.equals("batch"))
                .add("live", 0.7, str -> str.equals("live"))
                .build();
        
        strategy.setLimit(10);
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(strategy.tryAcquire("batch").isPresent());
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
        }
        
        Assert.assertFalse(strategy.tryAcquire("batch").isPresent());
        
        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(strategy.tryAcquire("live").isPresent());
            Assert.assertEquals(i+1, strategy.getBinBusyCount(1));
        }

        Assert.assertFalse(strategy.tryAcquire("live").isPresent());
    }

    @Test
    public void rejectOnceAllLimitsReached() {
        PredicatePartitionStrategy<String> strategy = PredicatePartitionStrategy.<String>newBuilder()
                .add("batch", 0.3, str -> str.equals("batch"))
                .add("live", 0.7, str -> str.equals("live"))
                .build();
        
        strategy.setLimit(10);
        for (int i = 0; i < 3; i++) {
            Assert.assertTrue(strategy.tryAcquire("batch").isPresent());
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
            Assert.assertEquals(i+1, strategy.getBusyCount());
        }
        
        for (int i = 0; i < 7; i++) {
            Assert.assertTrue(strategy.tryAcquire("live").isPresent());
            Assert.assertEquals(i+1, strategy.getBinBusyCount(1));
            Assert.assertEquals(i+4, strategy.getBusyCount());
        }

        Assert.assertFalse(strategy.tryAcquire("batch").isPresent());
        Assert.assertFalse(strategy.tryAcquire("live").isPresent());
    }

    @Test
    public void releaseLimit() {
        PredicatePartitionStrategy<String> strategy = PredicatePartitionStrategy.<String>newBuilder()
                .add("batch", 0.3, str -> str.equals("batch"))
                .add("live", 0.7, str -> str.equals("live"))
                .build();
        
        strategy.setLimit(10);
        Optional<Runnable> completion = strategy.tryAcquire("batch");
        for (int i = 1; i < 10; i++) {
            Assert.assertTrue(strategy.tryAcquire("batch").isPresent());
            Assert.assertEquals(i+1, strategy.getBinBusyCount(0));
        }
    
        Assert.assertEquals(10, strategy.getBusyCount());
        
        Assert.assertFalse(strategy.tryAcquire("batch").isPresent());
        
        completion.get().run();
        Assert.assertEquals(9, strategy.getBinBusyCount(0));
        Assert.assertEquals(9, strategy.getBusyCount());
        
        Assert.assertTrue(strategy.tryAcquire("batch").isPresent());
        Assert.assertEquals(10, strategy.getBinBusyCount(0));
        Assert.assertEquals(10, strategy.getBusyCount());
    }
    
    @Test
    public void setLimitReservesBusy() {
        PredicatePartitionStrategy<String> strategy = PredicatePartitionStrategy.<String>newBuilder()
                .add("batch", 0.3, str -> str.equals("batch"))
                .add("live", 0.7, str -> str.equals("live"))
                .build();
        
        strategy.setLimit(10);
        Assert.assertEquals(3, strategy.getBinLimit(0));
        Assert.assertTrue(strategy.tryAcquire("batch").isPresent());
        Assert.assertEquals(1, strategy.getBinBusyCount(0));
        Assert.assertEquals(1, strategy.getBusyCount());
        
        strategy.setLimit(20);
        Assert.assertEquals(6, strategy.getBinLimit(0));
        Assert.assertEquals(1, strategy.getBinBusyCount(0));
        Assert.assertEquals(1, strategy.getBusyCount());
    }
}
