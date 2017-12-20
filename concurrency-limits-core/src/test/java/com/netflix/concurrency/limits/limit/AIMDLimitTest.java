package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import junit.framework.Assert;

public class AIMDLimitTest {
    @Test
    public void testDefault() {
        AIMDLimit limiter = new AIMDLimit(10);
        Assert.assertEquals(10, limiter.getLimit());
    }
    
    @Test
    public void increaseOnSuccess() {
        AIMDLimit limiter = new AIMDLimit(10);
        limiter.update(TimeUnit.MILLISECONDS.toNanos(1));
        Assert.assertEquals(11, limiter.getLimit());
    }

    @Test
    public void decreaseOnDrops() {
        AIMDLimit limiter = new AIMDLimit(10);
        limiter.drop();
        limiter.update(TimeUnit.MILLISECONDS.toNanos(1));
        Assert.assertEquals(9, limiter.getLimit());
    }
}
