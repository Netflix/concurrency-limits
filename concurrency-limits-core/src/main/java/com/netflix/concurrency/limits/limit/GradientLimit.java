package com.netflix.concurrency.limits.limit;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;
import com.netflix.concurrency.limits.limit.functions.SquareRootFunction;

/**
 * Concurrency limit algorithm that adjust the limits based on the gradient of change in the 
 * samples minimum RTT and absolute minimum RTT allowing for a queue of square root of the 
 * current limit.  Why square root?  Because it's better than a fixed queue size that becomes too
 * small for large limits but still prevents the limit from growing too much by slowing down
 * growth as the limit grows.
 */
public final class GradientLimit implements Limit {
    private static final int DISABLED = -1;

    private static final Logger LOG = LoggerFactory.getLogger(GradientLimit.class);
    
    public static class Builder {
        private int initialLimit = 100;
        private int maxConcurrency = 1000;
        private long minRttThreshold = TimeUnit.MICROSECONDS.toNanos(1);
        
        private double smoothing = 0.2;
        private Function<Integer, Integer> queueSize = SquareRootFunction.create(4);
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private double rttTolerance = 1.0;
        
        private Supplier<Integer> resetRttCounterSupplier;
        
        private Builder() {
            probeNoLoadRtt(500, 1000);
        }

        /**
         * Minimum threshold for accepting a new rtt sample.  Any RTT lower than this threshold
         * will be discarded.
         *  
         * @param minRttTreshold
         * @param units
         * @return Chainable builder
         */
        public Builder minRttThreshold(long minRttTreshold, TimeUnit units) {
            this.minRttThreshold = units.toNanos(minRttTreshold);
            return this;
        }
        
        /**
         * Initial limit used by the limiter
         * @param initialLimit
         * @return Chainable builder
         */
        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }
        
        /**
         * Tolerance for changes in minimum latency.  
         * @param rttTolerance Value {@literal >}= 1.0 indicating how much change in minimum latency is acceptable
         *  before reducing the limit.  For example, a value of 2.0 means that a 2x increase in latency is acceptable. 
         * @return Chainable builder
         */
        public Builder rttTolerance(double rttTolerance) {
            Preconditions.checkArgument(rttTolerance >= 1.0, "Tolerance must be >= 1.0");
            this.rttTolerance = rttTolerance;
            return this;
        }
        
        /**
         * Maximum allowable concurrency.  Any estimated concurrency will be capped
         * at this value
         * @param maxConcurrency
         * @return Chainable builder
         */
        public Builder maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }
        
        /**
         * Fixed amount the estimated limit can grow while latencies remain low
         * @param queueSize
         * @return Chainable builder
         */
        public Builder queueSize(int queueSize) {
            this.queueSize = (ignore) -> queueSize;
            return this;
        }

        /**
         * Function to dynamically determine the amount the estimated limit can grow while
         * latencies remain low as a function of the current limit.
         * @param queueSize
         * @return Chainable builder
         */
        public Builder queueSize(Function<Integer, Integer> queueSize) {
            this.queueSize = queueSize;
            return this;
        }
        
        /**
         * Smoothing factor to limit how aggressively the estimated limit can shrink
         * when queuing has been detected.
         * @param smoothing Value of 0.0 to 1.0 where 1.0 means the limit is completely
         *  replicated by the new estimate.
         * @return Chainable builder
         */
        public Builder smoothing(double smoothing) {
            this.smoothing = smoothing;
            return this;
        }
        
        /**
         * Registry for reporting metrics about the limiter's internal state.
         * @param registry
         * @return Chainable builder
         */
        public Builder metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }
        
        /**
         * Probe for a new no_load RTT randomly in the range of [minUpdate, maxUpdates]
         * update intervals.  Default is [1000, 2000].
         * @param minUpdates
         * @param maxUpdates
         * @return Chinable builder
         */
        public Builder probeNoLoadRtt(int minUpdates, int maxUpdates) {
            Preconditions.checkArgument(minUpdates < maxUpdates, "minUpdates must be < maxUpdates");
            resetRttCounterSupplier = () -> ThreadLocalRandom.current().nextInt(minUpdates, maxUpdates);
            return this;
        }
        
        public GradientLimit build() {
            return new GradientLimit(this);
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
    
    private Measurement rttNoLoad = new MinimumMeasurement();
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final Function<Integer, Integer> queueSize;
    
    private final double smoothing;

    private final long minRttThreshold;

    private final double rttTolerance;

    private final SampleListener minRttSampleListener;

    private final SampleListener minWindowRttSampleListener;

    private final SampleListener queueSizeSampleListener;
    
    private final Supplier<Integer> resetRttCounterSupplier;
    
    private int resetRttCounter;
    
    private GradientLimit(Builder builder) {
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.queueSize = builder.queueSize;
        this.smoothing = builder.smoothing;
        this.minRttThreshold = builder.minRttThreshold;
        this.rttTolerance = builder.rttTolerance;
        this.resetRttCounterSupplier = builder.resetRttCounterSupplier;
        this.resetRttCounter = resetRttCounterSupplier.get();
        
        this.minRttSampleListener = builder.registry.registerDistribution(MetricIds.MIN_RTT_NAME);
        this.minWindowRttSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_MIN_RTT_NAME);
        this.queueSizeSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_QUEUE_SIZE_NAME);
    }

    @Override
    public synchronized void update(SampleWindow sample) {
        final long rtt = sample.getCandidateRttNanos();
        minWindowRttSampleListener.addSample(rtt);
        
        Preconditions.checkArgument(rtt > 0, "rtt must be >0 but got " + rtt);
        
        if (rtt < minRttThreshold) {
            return;
        }

        final double queueSize = this.queueSize.apply((int)this.estimatedLimit);
        queueSizeSampleListener.addSample(queueSize);

        // Reset or probe for a new RTT and a new estimatedLimit.  It's necessary to cut the limit
        // in half to avoid having the limit drift upwards when the RTT is probed during heavy load.
        // To avoid decreasing the limit too much we don't allow it to go lower than the queueSize.
        if (resetRttCounter != DISABLED && resetRttCounter-- <= 0) {
            resetRttCounter = this.resetRttCounterSupplier.get();
            
            estimatedLimit = Math.max(estimatedLimit - queueSize, queueSize);
            
            long nextRttNoLoad = rttNoLoad.update(current -> Math.min(current * 2, Math.max(current, rtt)));
            LOG.debug("Probe MinRTT {} limit={}", TimeUnit.NANOSECONDS.toMicros(nextRttNoLoad)/1000.0, getLimit());
            return;
        } else if (rttNoLoad.add(rtt)) {
            LOG.debug("New MinRTT {} limit={}", TimeUnit.NANOSECONDS.toMicros(rtt)/1000.0, getLimit());
        }
        
        minRttSampleListener.addSample(rttNoLoad.get());
        
        final double gradient = Math.max(0.5, Math.min(1.0, rttTolerance * rttNoLoad.get() / rtt));
        double newLimit;
        if (sample.didDrop()) {
            newLimit = estimatedLimit/2;
        } else if ((estimatedLimit - sample.getMaxInFlight()) > queueSize) {
            return;
        } else {
            newLimit = estimatedLimit * gradient + queueSize;
        }
        
        newLimit = Math.max(queueSize, Math.min(maxLimit, newLimit));
        if (newLimit < estimatedLimit) {
            newLimit = estimatedLimit * (1-smoothing) + smoothing*(newLimit);
        }
        if ((int)newLimit != (int)estimatedLimit) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("New limit={} minRtt={} ms winRtt={} ms queueSize={} gradient={} resetCounter={}", 
                        (int)newLimit, 
                        TimeUnit.NANOSECONDS.toMicros(rttNoLoad.get())/1000.0, 
                        TimeUnit.NANOSECONDS.toMicros(rtt)/1000.0,
                        queueSize,
                        gradient,
                        resetRttCounter);
            }
        }
        estimatedLimit = newLimit;
    }

    @Override
    public int getLimit() {
        return (int)estimatedLimit;
    }

    public long getRttNoLoad() {
        return rttNoLoad.get();
    }
    
    @Override
    public String toString() {
        return "GradientLimit [limit=" + (int)estimatedLimit + 
                ", rtt_noload=" + TimeUnit.MICROSECONDS.toMillis(rttNoLoad.get()) / 1000.0+
                " ms]";
    }
}
