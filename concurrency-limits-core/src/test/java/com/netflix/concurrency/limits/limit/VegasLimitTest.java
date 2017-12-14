package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import junit.framework.Assert;

public class VegasLimitTest {
    public static VegasLimit create() {
        return VegasLimit.newBuilder()
                .withAlpha(3)
                .withBeta(6)
                .withInitialLimit(10)
                .withMaxConcurrency(20)
                .build();
    }
    
    @Test
    public void initialLimit() {
        VegasLimit limit = create();
        Assert.assertEquals(10, limit.getLimit());
    }

    @Test
    public void increaseLimit() {
        VegasLimit limit = create();
        limit.update(TimeUnit.MILLISECONDS.toNanos(10));
        Assert.assertEquals(11, limit.getLimit());
        limit.update(TimeUnit.MILLISECONDS.toNanos(10));
        Assert.assertEquals(12, limit.getLimit());
    }
    
    @Test
    public void decreaseLimit() {
        VegasLimit limit = create();
        limit.update(TimeUnit.MILLISECONDS.toNanos(10));
        Assert.assertEquals(11, limit.getLimit());
        limit.update(TimeUnit.MILLISECONDS.toNanos(50));
        Assert.assertEquals(10, limit.getLimit());
    }
    
    @Test
    public void noChangeIfWithinThresholds() {
        VegasLimit limit = create();
        limit.update(TimeUnit.MILLISECONDS.toNanos(10));
        Assert.assertEquals(11, limit.getLimit());
        limit.update(TimeUnit.MILLISECONDS.toNanos(14));
        Assert.assertEquals(11, limit.getLimit());
    }
}
