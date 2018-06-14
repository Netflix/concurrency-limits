package com.netflix.concurrency.limits.limit;

public class ExpAvgMeasurement implements Measurement {
    private final double ratio;
    private final double filter;
    
    private double value;
    
    public ExpAvgMeasurement(int window, double filter) {
        this.ratio = 1.0 / window;
        this.filter = filter;
        this.value = 0.0;
    }
    
    @Override
    public Number add(Number sample) {
        if (value == 0.0) {
            value = sample.doubleValue();
        } else if (sample.doubleValue() < value) {
            value = sample.doubleValue();
        } else {
            value = (1-ratio) * value + ratio * Math.min(value*filter, sample.doubleValue());
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

}
