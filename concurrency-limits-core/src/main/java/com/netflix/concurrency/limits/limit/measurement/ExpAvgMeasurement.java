package com.netflix.concurrency.limits.limit.measurement;

import java.util.function.Function;

public class ExpAvgMeasurement implements Measurement {
    private Double value = 0.0;
    private final int window;
    private int count = 0;

    public ExpAvgMeasurement(int window) {
        this.window = window;
    }

    @Override
    public Number add(Number sample) {
        if (count == 0) {
            count++;
            value = sample.doubleValue();
        } else if (count < window) {
            count++;
            double factor = factor(count);
            value = value * (1-factor) + sample.doubleValue() * factor;
        } else {
            double factor = factor(window);
            value = value * (1-factor) + sample.doubleValue() * factor;
        }
        return value;
    }

    private static double factor(int n) {
        return 2.0 / (n + 1);
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

    @Override
    public void update(Function<Number, Number> operation) {
        this.value = operation.apply(value).doubleValue();
    }
}
