package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.internal.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * Limiter based on TCP Vegas where the limit increases by 1 if the queue_use is small (< alpha)
 * and decreases by 1 if the queue_use is large (> beta).
 * 
 * Queue size is calculated using the formula, 
 *  queue_use = limit − BWE×RTTnoLoad = limit × (1 − RTTnoLoad/RTTactual)
 *
 * alpha is typically 2-3 and beta is typically 4-6
 */
public class VegasLimit implements Limit {
    public static class Builder {
        private int initialLimit = 10;
        private int maxConcurrency = 100;
        private int alpha = 2;
        private int beta = 4;
        private double backoffRatio = 0.9;
        
        public Builder withAlpha(int alpha) {
            this.alpha = alpha;
            return this;
        }
        
        public Builder withBeta(int beta) {
            this.beta = beta;
            return this;
        }
        
        public Builder withInitialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }
        
        public Builder withMaxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }
        
        public Builder withBackoffRatio(double ratio) {
            this.backoffRatio = ratio;
            return this;
        }
        
        public VegasLimit build() {
            return new VegasLimit(this);
        }
    }
    
    public static Builder newBuilder() {
        return new Builder();
    }
    
    public static VegasLimit newDefault() {
        return newBuilder().build();
    }
    
    /**
     * Estimated concurrency limit based on our algorithm
     */
    private volatile int estimatedLimit;
    
    private long rtt_noload;
    
    private boolean didDrop = false;
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final int alpha;
    private final int beta;
    private final double backoffRatio;
    
    private VegasLimit(Builder builder) {
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.alpha = builder.alpha;
        this.beta = builder.beta;
        this.backoffRatio = builder.backoffRatio;
    }

    @Override
    public synchronized void update(long rtt) {
        Preconditions.checkArgument(rtt > 0, "rtt must be >0 but got " + rtt);
        
        if (rtt_noload == 0 || rtt < rtt_noload) {
            rtt_noload = rtt;
        }
        
        if (didDrop) {
            didDrop = false;
        } else {
            int newLimit = 1;
            int queueSize = (int) Math.ceil(estimatedLimit * (1 - (double)rtt_noload / rtt));
            if (queueSize <= alpha) {
                newLimit = estimatedLimit + 1;
            } else if (queueSize >= beta) {
                newLimit = estimatedLimit - 1;
            }
            
            estimatedLimit = Math.max(1, Math.min(maxLimit, newLimit));
        }
    }

    @Override
    public synchronized void drop() {
        if (!didDrop) {
            didDrop = true;
            estimatedLimit = Math.max(1, Math.min(estimatedLimit - 1, (int) (estimatedLimit * backoffRatio)));
        }
    }

    @Override
    public int getLimit() {
        return estimatedLimit;
    }

    @Override
    public String toString() {
        return "VegasLimit [limit=" + estimatedLimit + 
                ", rtt_noload=" + TimeUnit.NANOSECONDS.toMillis(rtt_noload) +
                "]";
    }
}
