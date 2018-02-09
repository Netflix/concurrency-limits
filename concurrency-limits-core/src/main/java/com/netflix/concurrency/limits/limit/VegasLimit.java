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
        private Function<Integer, Integer> alpha = (limit) -> Math.max(3, limit / 10);
        private Function<Integer, Integer> beta = (limit) -> Math.max(6, limit / 5);
        private double tolerance = 1.0;
        private double backoffRatio = 0.9;
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        
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
        
        /**
         * Tolerance multiple of the windowed RTT measurement when adjusting the limit down.
         * A value of 1 means little tolerance for changes in window RTT and the sample is used as 
         * is to determine queuing.  A value of 2 means that the window RTT may be double the 
         * absolute minimum before being considered 
         * @param tolerance
         * @return
         */
        public Builder tolerance(double tolerance) {
            Preconditions.checkArgument(tolerance >= 1, "Tolerance must be >= 1");
            this.tolerance = tolerance;
            return this;
        }
        
        @Deprecated
        public Builder fastStartEnabled(boolean fastStartEnabled) {
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
    
    private volatile long rtt_noload = 0;
    
    private boolean didDrop = false;
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final double tolerance;
    private final Function<Integer, Integer> alphaFunc;
    private final Function<Integer, Integer> betaFunc;
    private final double backoffRatio;

    private VegasLimit(Builder builder) {
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.alphaFunc = builder.alpha;
        this.betaFunc = builder.beta;
        this.backoffRatio = builder.backoffRatio;
        this.tolerance = builder.tolerance;
        builder.registry.registerGauge(MetricIds.MIN_RTT_GUAGE_NAME, () -> rtt_noload);
    }

    @Override
    public synchronized void update(long rtt) {
        Preconditions.checkArgument(rtt > 0, "rtt must be >0 but got " + rtt);
        
        if (rtt_noload == 0 || rtt < rtt_noload) {
            LOG.debug("New MinRTT {}", rtt);
            rtt_noload = rtt;
        }
        
        if (didDrop) {
            didDrop = false;
        } else {
            int newLimit = estimatedLimit;
            long adjusted_rtt = rtt < (rtt_noload * tolerance) ? rtt_noload : rtt; 
            int alpha = alphaFunc.apply(estimatedLimit);
            int beta = betaFunc.apply(estimatedLimit);
            int queueSize = (int) Math.ceil(estimatedLimit * (1 - (double)rtt_noload / adjusted_rtt));
            if (queueSize <= alpha) {
                newLimit += alpha;
            } else if (queueSize > beta) {
                newLimit -= alpha;
            }
            
            newLimit = Math.max(1, Math.min(maxLimit, newLimit));
            if (newLimit != estimatedLimit) {
                estimatedLimit = newLimit;
                LOG.debug("New limit={} minRtt={} μs winRtt={} μs queueSize={}", 
                        estimatedLimit, 
                        TimeUnit.NANOSECONDS.toMicros(rtt_noload), 
                        TimeUnit.NANOSECONDS.toMicros(rtt),
                        queueSize);
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
