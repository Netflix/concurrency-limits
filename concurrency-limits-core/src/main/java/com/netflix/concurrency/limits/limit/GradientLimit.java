package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
    private static final Logger LOG = LoggerFactory.getLogger(GradientLimit.class);
    
    public static class Builder {
        private int initialLimit = 50;
        private int minLimit = 1;
        private int maxLimit = 200;
        
        private double smoothing = 0.2;
        private Function<Integer, Integer> queueSize = SquareRootFunction.create(4);
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private int noLoadRttWindow = 1000;
        private double noLoadRttFilter = 1.1;
        private double rttTolerance = 2.0;
        
        /**
         * Minimum threshold for accepting a new rtt sample.  Any RTT lower than this threshold
         * will be discarded.
         *  
         * @param minRttTreshold
         * @param units
         * @return Chainable builder
         */
        @Deprecated
        public Builder minRttThreshold(long minRttTreshold, TimeUnit units) {
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
        @Deprecated
        public Builder maxConcurrency(int maxConcurrency) {
            return maxLimit(maxConcurrency);
        }

        /**
         * Maximum allowable concurrency.  Any estimated concurrency will be capped
         * at this value
         * @param maxLimit
         * @return Chainable builder
         */
        public Builder maxLimit(int maxLimit) {
            this.maxLimit = maxLimit;
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
         * The limiter will probe for a new noload RTT every probeMultiplier * current limit
         * iterations.  Default value is 30. Set to -1 to disable 
         * @param probeMultiplier 
         * @return Chainable builder
         */
        @Deprecated
        public Builder probeMultiplier(int probeMultiplier) {
            return this;
        }

        /**
         * Exponential moving average window size of sample updates for tracking noLoad RTT
         * Having sample window lets the system adapt to changes in latency characteristics.
         * @param window
         * @return Chainable builder
         */
        public Builder noLoadRttWindow(int window) {
            this.noLoadRttWindow = window;
            return this;
        }
        
        /**
         * Low pass filter applied to noLoad RTT measurements ensuring that outlier latency
         * measurements don't have an adverse impact on the noLoad rtt.
         * @param filter 
         * @return Chainable builder
         */
        public Builder noLoadRttFilter(double filter) {
            this.noLoadRttFilter = filter;
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
    
    private final Measurement rttNoLoadAccumulator;
    
    private final double smoothing;
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final int minLimit;
    
    private final double rttTolerance;
    
    private final Function<Integer, Integer> queueSize;
    
    private final SampleListener minRttSampleListener;

    private final SampleListener minWindowRttSampleListener;

    private final SampleListener queueSizeSampleListener;
    
    private GradientLimit(Builder builder) {
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxLimit;
        this.minLimit = builder.minLimit;
        this.queueSize = builder.queueSize;
        this.smoothing = builder.smoothing;
        this.rttTolerance = builder.rttTolerance;
        
        this.rttNoLoadAccumulator = new ExpAvgMeasurement(builder.noLoadRttWindow, builder.noLoadRttFilter);
        
        this.minRttSampleListener = builder.registry.registerDistribution(MetricIds.MIN_RTT_NAME);
        this.minWindowRttSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_MIN_RTT_NAME);
        this.queueSizeSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_QUEUE_SIZE_NAME);
    }

    @Override
    public synchronized void update(SampleWindow sample) {
        Preconditions.checkArgument(sample.getCandidateRttNanos() > 0, "rtt must be >0 but got " + sample.getCandidateRttNanos());
        
        final long rttSample = sample.getRttSumNanos() / sample.getSampleCount();
        minWindowRttSampleListener.addSample(rttSample);

        final double queueSize = this.queueSize.apply((int)this.estimatedLimit);
        queueSizeSampleListener.addSample(queueSize);

        final double rttNoLoad = rttNoLoadAccumulator.add(rttSample).doubleValue();
        
        minRttSampleListener.addSample(rttSample);
        
        final double gradient;
        final double rtt = (double)rttSample / rttTolerance;
        // rtt is lower than rtt_noload because of smoothing rtt noload updates
        // set to 1.0 to indicate no queueing
        if (rtt < rttNoLoad) {
            gradient = 1.0;
        } else {
            gradient = Math.max(0.5, rttNoLoad / rtt);
        }
        
        double newLimit;
        // Reduce the limit aggressively if there was a drop
        if (sample.didDrop()) {
            newLimit = estimatedLimit/2;
        // Normal update to the limit
        } else {
            newLimit = estimatedLimit * gradient + queueSize;
        }
        
        // Apply a smoothing factor when reducing the limit only
        if (newLimit < estimatedLimit) {
            newLimit = (1-smoothing) * estimatedLimit + smoothing*(newLimit);
        }
        
        newLimit = Math.max(Math.max(minLimit, queueSize), Math.min(maxLimit, newLimit));
            
        if (LOG.isDebugEnabled()) {
            LOG.debug("New limit={} minRtt={} ms winRtt={} ms queueSize={} gradient={}", 
                    (int)newLimit, 
                    TimeUnit.NANOSECONDS.toMicros((int)rttNoLoad)/1000.0, 
                    TimeUnit.NANOSECONDS.toMicros((int)rttSample)/1000.0,
                    queueSize,
                    gradient);
        }

        // We are app limited, don't increase the limit to prevent upward drift
        if (sample.getMaxInFlight() * 2 < estimatedLimit) {
            return;
        }
        
        estimatedLimit = newLimit;
    }

    @Override
    public int getLimit() {
        return (int)estimatedLimit;
    }

    public long getRttNoLoad() {
        return rttNoLoadAccumulator.get().longValue();
    }
    
    @Override
    public String toString() {
        return "GradientLimit [limit=" + (int)estimatedLimit + 
                ", rtt_noload=" + TimeUnit.MICROSECONDS.toMillis(getRttNoLoad()) / 1000.0+
                " ms]";
    }
}
