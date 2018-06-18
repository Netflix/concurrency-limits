package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;
import com.netflix.concurrency.limits.limit.functions.SquareRootFunction;

import java.util.concurrent.ThreadLocalRandom;
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
    private static final int DISABLED = -1;

    private static final Logger LOG = LoggerFactory.getLogger(GradientLimit.class);
    
    public static class Builder {
        private int initialLimit = 50;
        private int minLimit = 1;
        private int maxConcurrency = 1000;
        private long minRttThreshold = TimeUnit.MICROSECONDS.toNanos(1);
        
        private double smoothing = 0.2;
        private Function<Integer, Integer> queueSize = SquareRootFunction.create(4);
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private double rttTolerance = 2.0;
        private int probeInterval = 1000;
        
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
         * Minimum concurrency limit allowed.  The minimum helps prevent the algorithm from adjust the limit
         * too far down.  Note that this limit is not desirable when use as backpressure for batch apps.
         * 
         * @param minLimit
         * @return Chainable builder
         */
        public Builder minLimit(int minLimit) {
            this.minLimit = minLimit;
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
        
        @Deprecated
        public Builder probeMultiplier(int probeMultiplier) {
            return this;
        }

        /**
         * The limiter will probe for a new noload RTT every probeInterval
         * updates.  Default value is 1000. Set to -1 to disable 
         * @param probeInterval 
         * @return Chainable builder
         */
        public Builder probeInterval(int probeInterval) {
            this.probeInterval = probeInterval;
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
    
    private final Measurement rttNoLoad;
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final int minLimit;
    
    private final Function<Integer, Integer> queueSize;
    
    private final double smoothing;

    private final long minRttThreshold;

    private final double rttTolerance;

    private final SampleListener minRttSampleListener;

    private final SampleListener minWindowRttSampleListener;

    private final SampleListener queueSizeSampleListener;
    
    private final int probeInterval;
    
    private int resetRttCounter;
    
    private GradientLimit(Builder builder) {
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.minLimit = builder.minLimit;
        this.queueSize = builder.queueSize;
        this.smoothing = builder.smoothing;
        this.minRttThreshold = builder.minRttThreshold;
        this.rttTolerance = builder.rttTolerance;
        this.probeInterval = builder.probeInterval;
        this.resetRttCounter = nextProbeCountdown();
        this.rttNoLoad = new SmoothingMinimumMeasurement(builder.smoothing);
        
        this.minRttSampleListener = builder.registry.registerDistribution(MetricIds.MIN_RTT_NAME);
        this.minWindowRttSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_MIN_RTT_NAME);
        this.queueSizeSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_QUEUE_SIZE_NAME);
    }

    private int nextProbeCountdown() {
        if (probeInterval == DISABLED) {
            return DISABLED;
        }
        return probeInterval + ThreadLocalRandom.current().nextInt(probeInterval);
    }

    @Override
    public synchronized void update(SampleWindow sample) {
        Preconditions.checkArgument(sample.getCandidateRttNanos() > 0, "rtt must be >0 but got " + sample.getCandidateRttNanos());
        
        final long rtt = sample.getAverateRttNanos();
        if (rtt < minRttThreshold) {
            return;
        }
        
        minWindowRttSampleListener.addSample(rtt);

        final double queueSize = this.queueSize.apply((int)this.estimatedLimit);
        queueSizeSampleListener.addSample(queueSize);

        // Reset or probe for a new noload RTT and a new estimatedLimit.  It's necessary to cut the limit
        // in half to avoid having the limit drift upwards when the RTT is probed during heavy load.
        // To avoid decreasing the limit too much we don't allow it to go lower than the queueSize.
        if (probeInterval != DISABLED && resetRttCounter-- <= 0) {
            resetRttCounter = nextProbeCountdown();
            
            estimatedLimit = Math.max(minLimit, Math.max(estimatedLimit - queueSize, queueSize));
            rttNoLoad.update(current -> rtt);
            LOG.debug("Probe MinRTT limit={}", getLimit());
            return;
        } else if (rttNoLoad.add(rtt)) {
            LOG.debug("New MinRTT {} limit={}", TimeUnit.NANOSECONDS.toMicros(rtt)/1000.0, getLimit());
        }
        
        minRttSampleListener.addSample(rttNoLoad.get());
        
        final double gradient;
        // rtt is still higher than rtt_noload because of smoothing rtt noload updates
        // set to 1.0 to indicate no queueing
        if (rttNoLoad.get().doubleValue() > rtt) {
            gradient = 1.0;
        } else {
            gradient = Math.max(0.5, rttTolerance * rttNoLoad.get().doubleValue() / rtt);
        }
        
        double newLimit;
        // Reduce the limit aggressively if there was a drop
        if (sample.didDrop()) {
            newLimit = estimatedLimit/2;
        // Don't grow the limit because we are app limited
        } else if (sample.getMaxInFlight() + queueSize < estimatedLimit) {
            return;
        // Normal update to the limit
        } else {
            newLimit = estimatedLimit * gradient + queueSize;
        }
        
        newLimit = Math.max(minLimit, estimatedLimit * (1-smoothing) + smoothing*(newLimit));
        newLimit = Math.max(queueSize, Math.min(maxLimit, newLimit));
        
        if ((int)newLimit != (int)estimatedLimit) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("New limit={} minRtt={} ms winRtt={} ms queueSize={} gradient={} resetCounter={}", 
                        (int)newLimit, 
                        TimeUnit.NANOSECONDS.toMicros(rttNoLoad.get().longValue())/1000.0, 
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
        return rttNoLoad.get().longValue();
    }
    
    @Override
    public String toString() {
        return "GradientLimit [limit=" + (int)estimatedLimit + 
                ", rtt_noload=" + TimeUnit.MICROSECONDS.toMillis(rttNoLoad.get().longValue()) / 1000.0+
                " ms]";
    }
}
