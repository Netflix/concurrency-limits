package com.netflix.concurrency.limits.limit;

import java.util.function.Function;

/**
 * Measures the minimum value of a sample set but also adds a smoothing factor
 */
public class SmoothingMinimumMeasurement implements Measurement {
    private Double value = 0.0;
    private final double ratio;
    
    /**
     * @param smoothing Factor applied to the new measurement using the formula
     *          value = value * (1-smoothing) + newValue * smoothing
     */
    public SmoothingMinimumMeasurement(double smoothing) {
        this.ratio = smoothing;
    }
    
    @Override
    public boolean add(Number sample) {
        if (value == 0) {
            value = sample.doubleValue();
            return true;
        } else if (sample.doubleValue() < value.doubleValue()) {
            value = value * ratio + sample.doubleValue() * (1-ratio);
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
        double newValue = func.apply(value).doubleValue();
        value = value * (1-ratio) + newValue * ratio;
        return value;
    }
}
