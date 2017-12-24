package com.netflix.concurrency.limits.internal;

import com.netflix.concurrency.limits.MetricRegistry;

import java.util.function.Supplier;

public final class EmptyMetricRegistry implements MetricRegistry {
    public static final EmptyMetricRegistry INSTANCE = new EmptyMetricRegistry();
    
    private EmptyMetricRegistry() {}
    
    @Override
    public Metric metric(String id) {
        return value -> { };
    }

    @Override
    public Metric metric(String id, String tagName, String valueName) {
        return value -> { };
    }

    @Override
    public void guage(String id, Supplier<Number> supplier) {
    }

    @Override
    public void guage(String id, String tagName, String tagValue, Supplier<Number> supplier) {
    }
}
