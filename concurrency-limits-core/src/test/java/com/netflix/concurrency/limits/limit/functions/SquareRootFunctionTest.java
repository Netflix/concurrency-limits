package com.netflix.concurrency.limits.limit.functions;

import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

public class SquareRootFunctionTest {
    @Test
    public void confirm0Index() {
        Function<Integer, Integer> func = SquareRootFunction.create(4);
        Assert.assertEquals(4, func.apply(0).intValue());
    }
    
    @Test
    public void confirmMaxIndex() {
        Function<Integer, Integer> func = SquareRootFunction.create(4);
        Assert.assertEquals(31, func.apply(999).intValue());
    }
    
    @Test
    public void confirmOutofLookupRange() {
        Function<Integer, Integer> func = SquareRootFunction.create(4);
        Assert.assertEquals(33, func.apply(1105).intValue());
    }
}
