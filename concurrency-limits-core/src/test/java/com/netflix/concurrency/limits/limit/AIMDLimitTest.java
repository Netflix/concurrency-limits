package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import junit.framework.Assert;

public class AIMDLimitTest {
    @Test
    public void testDefault() {
        AIMDLimit limiter = AIMDLimit.newBuilder().initialLimit(10).build();
        Assert.assertEquals(10, limiter.getLimit());
    }
    
    @Test
    public void increaseOnSuccess() {
        AIMDLimit limiter = AIMDLimit.newBuilder().initialLimit(10).build();
        limiter.update(TimeUnit.MILLISECONDS.toNanos(1));
        Assert.assertEquals(11, limiter.getLimit());
    }

    @Test
    public void decreaseOnDrops() {
        AIMDLimit limiter = AIMDLimit.newBuilder().initialLimit(10).build();
        limiter.drop();
        limiter.update(TimeUnit.MILLISECONDS.toNanos(1));
        Assert.assertEquals(9, limiter.getLimit());
    }
}
