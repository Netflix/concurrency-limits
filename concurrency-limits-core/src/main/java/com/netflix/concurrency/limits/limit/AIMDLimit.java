package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

/**
 * Loss based dynamic {@link Limit} that does an additive increment as long as 
 * there are no errors and a multiplicative decrement when there is an error.
 */
public final class AIMDLimit extends AbstractLimit {
    
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
    
    private final double backoffRatio;

    private AIMDLimit(Builder builder) {
        super(builder.initialLimit);
        this.backoffRatio = builder.backoffRatio;
    }
    
    @Override
    protected int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        if (didDrop) {
            return Math.max(1, Math.min(getLimit() - 1, (int) (getLimit() * backoffRatio)));
        } else if (inflight >= getLimit()) {
            return getLimit() + 1;
        } else {
            return getLimit();
        }
    }

    @Override
    public String toString() {
        return "AIMDLimit [limit=" + getLimit() + "]";
    }
}
