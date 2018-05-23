package com.netflix.concurrency.limits.limit;

import java.util.function.Function;

public class MinimumMeasurement implements Measurement {
    private Double value = 0.0;
    
    @Override
    public boolean add(Number sample) {
        if (value == 0.0 || sample.doubleValue() < value) {
            value = sample.doubleValue();
            return true;
        }
        return false;
    }

    @Override
    public Number get() {
        return value;
    }

    @Override
    public void reset() {
        value = 0.0;
    }

    @Override
    public Number update(Function<Number, Number> func) {
        value = func.apply(value).doubleValue();
        return value;
    }
}
