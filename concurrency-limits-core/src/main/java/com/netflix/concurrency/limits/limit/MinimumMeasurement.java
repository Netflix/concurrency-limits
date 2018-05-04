package com.netflix.concurrency.limits.limit;

import java.util.function.Function;

public class MinimumMeasurement implements Measurement {
    private long value = 0;
    
    @Override
    public boolean add(long sample) {
        if (value == 0 || sample < value) {
            value = sample;
            return true;
        }
        return false;
    }

    @Override
    public long get() {
        return value;
    }

    @Override
    public void reset() {
        value = 0;
    }

    @Override
    public long update(Function<Long, Long> func) {
        value = func.apply(value);
        return value;
    }
}
