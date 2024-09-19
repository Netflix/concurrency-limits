package com.netflix.concurrency.limits.limit.functions;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.IntUnaryOperator;

public class Log10RootIntFunctionTest {
    @Test
    public void test0Index() {
        IntUnaryOperator func = Log10RootIntFunction.create(0);
        Assert.assertEquals(1, func.applyAsInt(0));
    }

    @Test
    public void testInRange() {
        IntUnaryOperator func = Log10RootIntFunction.create(0);
        Assert.assertEquals(2, func.applyAsInt(100));
    }

    @Test
    public void testOutofLookupRange() {
        IntUnaryOperator func = Log10RootIntFunction.create(0);
        Assert.assertEquals(4, func.applyAsInt(10000));
    }
}