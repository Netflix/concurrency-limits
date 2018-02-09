package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concurrency limit algorithm that adjust the limits based on the gradient of change in the 
 * samples minimum RTT and absolute minimum RTT allowing for a queue of square root of the 
 * current limit.  Why square root?  Because it's better than a fixed queue size that becomes too
 * small for large limits but still prevents the limit from growing too much by slowing down
 * growth as the limit grows.
 */
public final class GradientLimit implements Limit {
    private static final Logger LOG = LoggerFactory.getLogger(GradientLimit.class);
    
    public static class Builder {
        private int initialLimit = 20;
        private int maxConcurrency = 1000;
        private double smoothing = 0.2;
        private Function<Integer, Integer> queueSize = (limit) -> (int)Math.max(4, Math.sqrt(limit));
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        
        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }
        
        public Builder maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }
        
        public Builder queueSize(int queueSize) {
            this.queueSize = (ignore) -> queueSize;
            return this;
        }
        
        public Builder queueSize(Function<Integer, Integer> queueSize) {
            this.queueSize = queueSize;
            return this;
        }
        
        public Builder smoothing(double smoothing) {
            this.smoothing = smoothing;
            return this;
        }
        
        public Builder metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }
        
        public GradientLimit build() {
            GradientLimit limit = new GradientLimit(this);
            registry.registerGauge(MetricIds.MIN_RTT_GUAGE_NAME, limit::getRttNoLoad);
            return limit;
        }
    }
    
    public static Builder newBuilder() {
        return new Builder();
    }
    
    public static GradientLimit newDefault() {
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
    
    private final Function<Integer, Integer> queueSize;
    
    private final double smoothing;

    private GradientLimit(Builder builder) {
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.queueSize = builder.queueSize;
        this.smoothing = builder.smoothing;
    }

    @Override
    public synchronized void update(long rtt) {
        Preconditions.checkArgument(rtt > 0, "rtt must be >0 but got " + rtt);
        
        if (rtt_noload == 0 || rtt < rtt_noload) {
            LOG.debug("New MinRTT {}", rtt);
            rtt_noload = rtt;
        }

        final double queueSize = this.queueSize.apply((int) Math.ceil(this.estimatedLimit));
        final double gradient = (double)rtt_noload / rtt;
        double newLimit;
        if (didDrop) {
            newLimit = estimatedLimit * (1-smoothing) + smoothing*(estimatedLimit/2);
            didDrop = false;
        } else {
            newLimit = estimatedLimit * (1-smoothing) + smoothing*(gradient * estimatedLimit + queueSize);
        }
        
        newLimit = Math.max(1, Math.min(maxLimit, newLimit));
        if ((int)newLimit != (int)estimatedLimit) {
            estimatedLimit = newLimit;
            if (LOG.isDebugEnabled()) {
                LOG.debug("New limit={} minRtt={} μs winRtt={} μs queueSize={} gradient={}", 
                        (int)estimatedLimit, 
                        TimeUnit.NANOSECONDS.toMicros(rtt_noload), 
                        TimeUnit.NANOSECONDS.toMicros(rtt),
                        queueSize,
                        gradient);
            }
        }
    }

    @Override
    public synchronized void drop() {
        didDrop = true;
    }

    @Override
    public int getLimit() {
        return (int)Math.ceil(estimatedLimit);
    }

    public long getRttNoLoad() {
        return rtt_noload;
    }
    
    @Override
    public String toString() {
        return "GradientLimit [limit=" + (int)estimatedLimit + 
                ", rtt_noload=" + TimeUnit.NANOSECONDS.toMillis(rtt_noload) +
                "]";
    }
}
