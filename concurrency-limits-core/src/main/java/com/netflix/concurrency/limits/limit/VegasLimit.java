package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;

/**
 * Limiter based on TCP Vegas where the limit increases by alpha if the queue_use is small ({@literal <} alpha)
 * and decreases by alpha if the queue_use is large ({@literal >} beta).
 * 
 * Queue size is calculated using the formula, 
 *  queue_use = limit − BWE×RTTnoLoad = limit × (1 − RTTnoLoad/RTTactual)
 *
 * For traditional TCP Vegas alpha is typically 2-3 and beta is typically 4-6.  To allow for better growth and 
 * stability at higher limits we set alpha=Max(3, 10% of the current limit) and beta=Max(6, 20% of the current limit)
 */
public class VegasLimit implements Limit {
    private static final Logger LOG = LoggerFactory.getLogger(VegasLimit.class);
    
    public static class Builder {
        private int initialLimit = 20;
        private int maxConcurrency = 1000;
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private double smoothing = 0.2;
        
        private Function<Integer, Integer> alpha = (limit) -> 3;
        private Function<Integer, Integer> beta = (limit) -> 6;
        private Function<Integer, Integer> increaseFunc = (limit) -> limit + 1;
        private Function<Integer, Integer> decreaseFunc = (limit) -> limit - 1;
        
        public Builder alpha(int alpha) {
            this.alpha = (ignore) -> alpha;
            return this;
        }
        
        public Builder alpha(Function<Integer, Integer> alpha) {
            this.alpha = alpha;
            return this;
        }
        
        public Builder beta(int beta) {
            this.beta = (ignore) -> beta;
            return this;
        }
        
        public Builder beta(Function<Integer, Integer> beta) {
            this.beta = beta;
            return this;
        }
        
        public Builder increase(Function<Integer, Integer> increase) {
            this.increaseFunc = increase;
            return this;
        }
        
        public Builder decrease(Function<Integer, Integer> decrease) {
            this.decreaseFunc = decrease;
            return this;
        }
        
        public Builder smoothing(double smoothing) {
            this.smoothing = smoothing;
            return this;
        }
        
        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }
        
        @Deprecated
        public Builder tolerance(double tolerance) {
            return this;
        }
        
        public Builder maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }
        
        @Deprecated
        public Builder backoffRatio(double ratio) {
            return this;
        }
        
        public Builder metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }
        
        public VegasLimit build() {
            VegasLimit limit = new VegasLimit(this);
            registry.registerGauge(MetricIds.MIN_RTT_GUAGE_NAME, limit::getRttNoLoad);
            return limit;
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
    private volatile double estimatedLimit;
    
    private volatile long rtt_noload = 0;
    
    private boolean didDrop = false;
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final double smoothing;
    private final Function<Integer, Integer> alphaFunc;
    private final Function<Integer, Integer> betaFunc;
    private final Function<Integer, Integer> increaseFunc;
    private final Function<Integer, Integer> decreaseFunc;

    private VegasLimit(Builder builder) {
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.alphaFunc = builder.alpha;
        this.betaFunc = builder.beta;
        this.increaseFunc = builder.increaseFunc;
        this.decreaseFunc = builder.decreaseFunc;
        this.smoothing = builder.smoothing;
    }

    @Override
    public synchronized void update(long rtt, int maxInFlight) {
        Preconditions.checkArgument(rtt > 0, "rtt must be >0 but got " + rtt);
        
        if (rtt_noload == 0 || rtt < rtt_noload) {
            LOG.debug("New MinRTT {}", rtt);
            rtt_noload = rtt;
        }
        
        double newLimit;
        final int queueSize = (int) Math.ceil(estimatedLimit * (1 - (double)rtt_noload / rtt));
        if (didDrop) {
            newLimit = decreaseFunc.apply((int)estimatedLimit);
            didDrop = false;
        } else if (maxInFlight < estimatedLimit) {
            return;
        } else {
            int alpha = alphaFunc.apply((int)estimatedLimit);
            int beta = betaFunc.apply((int)estimatedLimit);
            
            if (queueSize < alpha) {
                newLimit = increaseFunc.apply((int)estimatedLimit);
            } else if (queueSize > beta) {
                newLimit = decreaseFunc.apply((int)estimatedLimit);
            } else {
                return;
            }
        }

        newLimit = Math.max(1, Math.min(maxLimit, newLimit));
        newLimit = (int) ((1 - smoothing) * estimatedLimit + smoothing * newLimit);
        if ((int)newLimit != (int)estimatedLimit && LOG.isDebugEnabled()) {
            LOG.debug("New limit={} minRtt={} μs winRtt={} μs queueSize={}", 
                    estimatedLimit, 
                    TimeUnit.NANOSECONDS.toMicros(rtt_noload), 
                    TimeUnit.NANOSECONDS.toMicros(rtt),
                    queueSize);
        }
        estimatedLimit = newLimit;
    }

    @Override
    public synchronized void drop() {
        if (!didDrop) {
            didDrop = true;
        }
    }

    @Override
    public int getLimit() {
        return (int)estimatedLimit;
    }

    long getRttNoLoad() {
        return rtt_noload;
    }
    
    @Override
    public String toString() {
        return "VegasLimit [limit=" + estimatedLimit + 
                ", rtt_noload=" + TimeUnit.NANOSECONDS.toMillis(rtt_noload) +
                "]";
    }
}
