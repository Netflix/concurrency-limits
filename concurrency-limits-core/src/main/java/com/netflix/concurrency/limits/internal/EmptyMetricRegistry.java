package com.netflix.concurrency.limits.internal;

import com.netflix.concurrency.limits.MetricRegistry;

import java.util.function.Supplier;

public final class EmptyMetricRegistry implements MetricRegistry {
    public static final EmptyMetricRegistry INSTANCE = new EmptyMetricRegistry();
    
    private EmptyMetricRegistry() {}
    
    @Override
    public SampleListener registerDistribution(String id, String... tagNameValuePairs) {
        return value -> { };
    }

    @Override
    public void registerGuage(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
    }
}
