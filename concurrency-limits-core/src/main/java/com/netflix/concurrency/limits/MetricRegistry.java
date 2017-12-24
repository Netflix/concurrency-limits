package com.netflix.concurrency.limits;

import java.util.function.Supplier;

/**
 * Simple abstraction for create metrics
 */
public interface MetricRegistry {
    interface Metric {
        void add(Number value);
    }
    
    Metric metric(String id);
    Metric metric(String id, String tagName, String valueName);

    void guage(String id, Supplier<Number> supplier);
    void guage(String id, String tagName, String tagValue, Supplier<Number> supplier);
}
