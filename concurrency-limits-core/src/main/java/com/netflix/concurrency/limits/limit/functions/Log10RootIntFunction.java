package com.netflix.concurrency.limits.limit.functions;

import java.util.function.IntUnaryOperator;

/**
 * Function used by limiters to calculate thresholds using log10 of the current limit.
 * Here we pre-compute the log10 of numbers up to 1000 as an optimization.
 */
public final class Log10RootIntFunction implements IntUnaryOperator {

    private Log10RootIntFunction() {}

    private static final int[] lookup = new int[1000];

    static {
        for (int i = 0; i < lookup.length; i++) {
            lookup[i] = Math.max(1, (int) Math.log10(i));
        }
    }

    private static final Log10RootIntFunction INSTANCE = new Log10RootIntFunction();

    /**
     * Create an instance of a function that returns : baseline + sqrt(limit)
     *
     * @param baseline
     * @return
     */
    public static IntUnaryOperator create(int baseline) {
        return baseline == 0 ? INSTANCE : INSTANCE.andThen(t -> t + baseline);
    }

    @Override
    public int applyAsInt(int t) {
        return t < 1000 ? lookup[t] : (int) Math.log10(t);
    }
}
