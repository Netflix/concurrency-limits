package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.netflix.concurrency.limits.limiter.ImmutableSample;

import junit.framework.Assert;

public class VegasLimitTest {
    public static VegasLimit create() {
        return VegasLimit.newBuilder()
                .alpha(3)
                .beta(6)
                .smoothing(1.0)
                .initialLimit(10)
                .maxConcurrency(20)
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
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(10), 10L));
        Assert.assertEquals(11, limit.getLimit());
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(10), 11L));
        Assert.assertEquals(12, limit.getLimit());
    }
    
    @Test
    public void decreaseLimit() {
        VegasLimit limit = create();
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(10), 10L));
        Assert.assertEquals(11, limit.getLimit());
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(50), 11L));
        Assert.assertEquals(10, limit.getLimit());
    }
    
    @Test
    public void noChangeIfWithinThresholds() {
        VegasLimit limit = create();
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(10), 10L));
        Assert.assertEquals(11, limit.getLimit());
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(14), 14L));
        Assert.assertEquals(11, limit.getLimit());
    }
    
    @Test
    public void decreaseSmoothing() {
        VegasLimit limit = VegasLimit.newBuilder()
            .decrease(current -> current / 2)
            .smoothing(0.5)
            .initialLimit(100)
            .maxConcurrency(200)
            .build();
        
        // Pick up first min-rtt
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(10), 100L));
        Assert.assertEquals(100, limit.getLimit());
        
        // First decrease
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(20), 100L));
        Assert.assertEquals(75, limit.getLimit());

        // Second decrease
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(20), 100L));
        Assert.assertEquals(56, limit.getLimit());
    }

    @Test
    public void decreaseWithoutSmoothing() {
        VegasLimit limit = VegasLimit.newBuilder()
            .decrease(current -> current / 2)
            .initialLimit(100)
            .maxConcurrency(200)
            .build();
        
        // Pick up first min-rtt
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(10), 100L));
        Assert.assertEquals(101, limit.getLimit());
        
        // First decrease
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(20), 100L));
        Assert.assertEquals(50, limit.getLimit());

        // Second decrease
        limit.update(new ImmutableSample().addSample(TimeUnit.MILLISECONDS.toNanos(20), 100L));
        Assert.assertEquals(25, limit.getLimit());
    }
}
