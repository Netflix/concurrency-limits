package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dynamic limiter that does an additive increment as long as there are no errors
 * and a multiplicative decrement when there are error.
 */
public final class AIMDLimit implements Limit {

    private AtomicReference<Double> limit = new AtomicReference<>();
    private AtomicBoolean didDrop = new AtomicBoolean(false);
    
    public AIMDLimit(int initialLimit) {
        this.limit.set(Integer.valueOf(initialLimit).doubleValue());
    }
    
    @Override
    public int getLimit() {
        return limit.get().intValue();
    }

    @Override
    public int update(long rtt) {
        final Double current = limit.get();
        if (didDrop.compareAndSet(true, false)) {
            limit.compareAndSet(current, Math.max(1, current * 0.75));
        } else {
            limit.compareAndSet(current, current + 1);
        }

        return getLimit();
    }

    @Override
    public int drop() {
        didDrop.set(true);
        return getLimit();
    }
}
