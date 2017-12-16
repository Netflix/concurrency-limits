package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

/**
 * Loss based dynamic {@link Limit} that does an additive increment as long as 
 * there are no errors and a multiplicative decrement when there is an error.
 */
public final class AIMDLimit implements Limit {

    private static final double DEFAULT_BACKOFF_RATIO = 0.9;
    
    private volatile int limit;
    private boolean didDrop = false;
    private final double backoffRatio;
    
    public AIMDLimit(int initialLimit) {
        this(initialLimit, DEFAULT_BACKOFF_RATIO);
    }
    
    public AIMDLimit(int initialLimit, double backoffRatio) {
        this.limit = initialLimit;
        this.backoffRatio = backoffRatio;
    }
    
    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public synchronized void update(long rtt) {
        if (didDrop) {
            didDrop = false;
        } else {
            limit = limit + 1;
        }
    }

    @Override
    public synchronized void drop() {
        if (!didDrop) {
            didDrop = true;
            limit = Math.max(1, Math.min(limit - 1, (int) (limit * backoffRatio)));
        }
    }

    @Override
    public String toString() {
        return "AIMDLimit [limit=" + limit + ", didDrop=" + didDrop + "]";
    }
}
