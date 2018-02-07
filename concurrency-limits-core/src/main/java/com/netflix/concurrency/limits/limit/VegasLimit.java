package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;

/**
 * Limiter based on TCP Vegas where the limit increases by 1 if the queue_use is small ({@literal <} alpha)
 * and decreases by 1 if the queue_use is large ({@literal >} beta).
 * 
 * Queue size is calculated using the formula, 
 *  queue_use = limit − BWE×RTTnoLoad = limit × (1 − RTTnoLoad/RTTactual)
 *
 * alpha is typically 2-3 and beta is typically 4-6
 */
public class VegasLimit implements Limit {
    private static final Logger LOG = LoggerFactory.getLogger(VegasLimit.class);
    
    public static class Builder {
        private int initialLimit = 20;
        private int maxConcurrency = 1000;
        private int alpha = 3;
        private int beta = 6;
        private double backoffRatio = 0.9;
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private boolean fastStartEnabled = false;
        
        public Builder alpha(int alpha) {
            this.alpha = alpha;
            return this;
        }
        
        public Builder beta(int beta) {
            this.beta = beta;
            return this;
        }
        
        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }
        
        public Builder maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }
        
        public Builder backoffRatio(double ratio) {
            this.backoffRatio = ratio;
            return this;
        }
        
        public Builder metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }
        
        public Builder fastStartEnabled(boolean fastStartEnabled) {
            this.fastStartEnabled = fastStartEnabled;
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
    
    private volatile long rtt_noload;
    
    private boolean didDrop = false;
    
    private boolean fastStart;

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
        this.fastStart = builder.fastStartEnabled;
        builder.registry.registerGauge(MetricIds.MIN_RTT_GUAGE_NAME, () -> rtt_noload);
    }

    @Override
    public synchronized void update(long rtt) {
        Preconditions.checkArgument(rtt > 0, "rtt must be >0 but got " + rtt);
        
        if (rtt_noload == 0 || rtt < rtt_noload) {
            LOG.info("MinRTT {}", rtt);
            rtt_noload = rtt;
        }
        
        if (didDrop) {
            didDrop = false;
            fastStart = false;
        } else {
            int newLimit = estimatedLimit;
            int queueSize = (int) Math.ceil(estimatedLimit * (1 - (double)rtt_noload / rtt));
            if (queueSize <= alpha) {
                if (fastStart) {
                    newLimit = Math.min(maxLimit, estimatedLimit * 2);
                } else {
                    newLimit ++;
                }
            } else if (queueSize > beta) {
                fastStart = false;
                newLimit --;
            }
            
            newLimit = Math.max(1, Math.min(maxLimit, newLimit));
            if (newLimit != estimatedLimit) {
                LOG.info("Limit {}", estimatedLimit);
                estimatedLimit = newLimit;
            }
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
