package com.netflix.concurrency.limits.strategy;

import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Simplest strategy for enforcing a concurrency limit that has a single counter
 * for tracking total usage.
 */
public final class SimpleStrategy<T> implements Strategy<T> {

    // A test hook to verify proper concurrency controls.
    private final Runnable acquireBarrier;
    private final AtomicInteger busy = new AtomicInteger();
    private volatile int limit = 1;
    private final SampleListener inflightMetric;
    
    public SimpleStrategy() {
        this(EmptyMetricRegistry.INSTANCE);
    }
    
    public SimpleStrategy(MetricRegistry registry) {
        this(registry, () -> {});
    }

    // Test Only
    SimpleStrategy(MetricRegistry registry, Runnable acquireBarrier) {
        this.inflightMetric = registry.registerDistribution(MetricIds.INFLIGHT_GUAGE_NAME);
        registry.registerGauge(MetricIds.LIMIT_GUAGE_NAME, this::getLimit);
        this.acquireBarrier = acquireBarrier;
    }
    
    @Override
    public Token tryAcquire(T context) {
        // Spin in a CAS loop to guard against modifications after reading.
        while (true) {
            final int currentBusy = busy.get();
            acquireBarrier.run();
            if (currentBusy >= limit) {
                inflightMetric.addSample(currentBusy);
                return Token.newNotAcquired(currentBusy);
            }
            final int inflight = currentBusy + 1;
            if (busy.compareAndSet(currentBusy, inflight)) {
                inflightMetric.addSample(inflight);
                return Token.newAcquired(inflight, busy::decrementAndGet);
            }
        }
    }
    
    @Override
    public void setLimit(int limit) {
        if (limit < 1) {
            limit = 1;
        }
        this.limit = limit;
    }
    
    // Visible for testing
    int getLimit() {
        return limit;
    }
    
    int getBusyCount() {
        return busy.get();
    }

    @Override
    public String toString() {
        return "SimpleStrategy [busy=" + busy.get() + ", limit=" + limit + "]";
    }
}
