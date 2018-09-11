package com.netflix.concurrency.limits.limit.measurement;

import java.util.function.Function;

public class MinimumMeasurement implements Measurement {
    private Double value = 0.0;
    
    @Override
    public Number add(Number sample) {
        if (value == 0.0 || sample.doubleValue() < value) {
            value = sample.doubleValue();
        }
        return value;
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
    public void update(Function<Number, Number> operation) {
        
    }
}
