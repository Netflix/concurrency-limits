package com.netflix.concurrency.limits.limit;

public class ExpAvgMeasurement implements Measurement {
    private final int window;
    private final double ratio;
    private final double filter;
    
    private double value;
    private int count;
    
    public ExpAvgMeasurement(int window, double filter) {
        this.window = window;
        this.ratio = 1.0 / window;
        this.filter = filter;
        this.value = 0.0;
        this.count = 0;
    }
    
    @Override
    public Number add(Number sample) {
        // First sample seen
        if (count == 0) {
            value = sample.doubleValue();
            count = 1;
        // Adaptive average for the first <window> samples
        } else if (count < window) {
            count++;
            double tempRatio = 1.0 / count;
            value = (1-tempRatio) * value + tempRatio * Math.min(value*filter, sample.doubleValue());
        // Steady state
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
        count = 0;
    }

}
