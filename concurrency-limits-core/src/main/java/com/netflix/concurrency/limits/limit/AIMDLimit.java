package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

/**
 * Loss based dynamic {@link Limit} that does an additive increment as long as 
 * there are no errors and a multiplicative decrement when there is an error.
 */
public final class AIMDLimit implements Limit {
    
    public static class Builder {
        private int initialLimit = 10;
        private double backoffRatio = 0.9;
        
        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }
        
        public Builder backoffRatio(double backoffRatio) {
            this.backoffRatio = backoffRatio;
            return this;
        }
        
        public AIMDLimit build() {
            return new AIMDLimit(this);
        }
    }
    
    public static Builder newBuilder() {
        return new Builder();
    }
    
    private volatile int limit;
    private final double backoffRatio;

    private AIMDLimit(Builder builder) {
        this.limit = builder.initialLimit;
        this.backoffRatio = builder.backoffRatio;
    }
    
    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public void update(SampleWindow sample) {
        if (sample.didDrop()) {
            limit = Math.max(1, Math.min(limit - 1, (int) (limit * backoffRatio)));
        } else if (sample.getMaxInFlight() >= limit) {
            limit = limit + 1;
        }
    }

    @Override
    public String toString() {
        return "AIMDLimit [limit=" + limit + "]";
    }
}
