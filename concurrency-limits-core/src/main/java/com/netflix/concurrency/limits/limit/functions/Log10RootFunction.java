package com.netflix.concurrency.limits.limit.functions;

import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Function used by limiters to calculate thredsholds using log10 of the current limit.  
 * Here we pre-compute the log10 of numbers up to 1000 as an optimization.
 */
public final class Log10RootFunction implements Function<Integer, Integer> {
    static final int[] lookup = new int[1000];
    
    static {
        IntStream.range(0, 1000).forEach(i -> lookup[i] = Math.max(1, (int)Math.log10(i)));
    }
    
    private static final Log10RootFunction INSTANCE = new Log10RootFunction();
    
    /**
     * Create an instance of a function that returns : baseline + sqrt(limit)
     * 
     * @param baseline
     * @return
     */
    public static Function<Integer, Integer> create(int baseline) {
        return INSTANCE.andThen(t -> t + baseline);
    }
    
    @Override
    public Integer apply(Integer t) {
        return t < 1000 ? lookup[t] : (int)Math.sqrt(t);
    }
}
