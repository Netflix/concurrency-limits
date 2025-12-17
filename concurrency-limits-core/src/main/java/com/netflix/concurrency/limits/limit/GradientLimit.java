/**
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.Tags;
import com.netflix.concurrency.limits.internal.Preconditions;
import com.netflix.concurrency.limits.limit.functions.SquareRootFunction;
import com.netflix.concurrency.limits.limit.measurement.Measurement;
import com.netflix.concurrency.limits.limit.measurement.MinimumMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Concurrency limit algorithm that adjust the limits based on the gradient of change in the 
 * samples minimum RTT and absolute minimum RTT allowing for a queue of square root of the 
 * current limit.  Why square root?  Because it's better than a fixed queue size that becomes too
 * small for large limits but still prevents the limit from growing too much by slowing down
 * growth as the limit grows.
 */
public final class GradientLimit extends AbstractLimit {
    private static final int DISABLED = -1;

    private static final Logger LOG = LoggerFactory.getLogger(GradientLimit.class);
    
    public static class Builder extends AbstractLimit.Builder<Builder>{
        private int minLimit = 1;
        private int maxConcurrency = 1000;

        private double smoothing = 0.2;
        private Function<Integer, Integer> queueSize = SquareRootFunction.create(4);
        private double rttTolerance = 2.0;
        private int probeInterval = 1000;
        private double backoffRatio = 0.9;

        public Builder() {
            super(50);
        }
        
        /**
         * Minimum threshold for accepting a new rtt sample.  Any RTT lower than this threshold
         * will be discarded.
         *  
         * @param minRttThreshold
         * @param units
         * @return Chainable builder
         */
        @Deprecated
        public Builder minRttThreshold(long minRttThreshold, TimeUnit units) {
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
         * Ratio applied to the limit when a timeout was identified within the sampling window.  The default value is
         * 0.9.  A value of 1.0 means no backoff.
         * @param backoffRatio
         * @return
         */
        public Builder backoffRatio(double backoffRatio) {
            Preconditions.checkArgument(backoffRatio >= 0.5 && backoffRatio <= 1.0, "backoffRatio must be in the range [0.5, 1.0]");
            this.backoffRatio = backoffRatio;
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

        @Override
        protected Builder self() {
            return this;
        }

        public GradientLimit build() {
            if (initialLimit > maxConcurrency) {
                LOG.warn("Initial limit {} exceeded maximum limit {}", initialLimit, maxConcurrency);
            }
            if (initialLimit < minLimit) {
                LOG.warn("Initial limit {} is less than minimum limit {}", initialLimit, minLimit);
            }
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

    private long lastRtt = 0;

    private final Measurement rttNoLoadMeasurement;
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final int minLimit;
    
    private final Function<Integer, Integer> queueSize;
    
    private final double smoothing;

    private final double rttTolerance;

    private final double backoffRatio;

    private final SampleListener minRttSampleListener;

    private final SampleListener minWindowRttSampleListener;

    private final SampleListener queueSizeSampleListener;
    
    private final int probeInterval;

    private int resetRttCounter;
    
    private GradientLimit(Builder builder) {
        super(builder);
        this.maxLimit = builder.maxConcurrency;
        this.minLimit = builder.minLimit;
        this.queueSize = builder.queueSize;
        this.smoothing = builder.smoothing;
        this.rttTolerance = builder.rttTolerance;
        this.backoffRatio = builder.backoffRatio;
        this.probeInterval = builder.probeInterval;
        this.resetRttCounter = nextProbeCountdown();
        this.rttNoLoadMeasurement = new MinimumMeasurement();
        
        this.minRttSampleListener = builder.registry.distribution(MetricIds.MIN_RTT_NAME, Tags.ID_NAME, builder.name);
        this.minWindowRttSampleListener = builder.registry.distribution(MetricIds.WINDOW_MIN_RTT_NAME, Tags.ID_NAME, builder.name);
        this.queueSizeSampleListener = builder.registry.distribution(MetricIds.WINDOW_QUEUE_SIZE_NAME, Tags.ID_NAME, builder.name);
    }

    private int nextProbeCountdown() {
        if (probeInterval == DISABLED) {
            return DISABLED;
        }
        return probeInterval + ThreadLocalRandom.current().nextInt(probeInterval);
    }

    @Override
    public int _update(final long startTime, final long rtt, final int inflight, final boolean didDrop) {
        lastRtt = rtt;
        minWindowRttSampleListener.addLongSample(rtt);

        final double queueSize = this.queueSize.apply((int)this.estimatedLimit);
        queueSizeSampleListener.addDoubleSample(queueSize);

        // Reset or probe for a new noload RTT and a new estimatedLimit.  It's necessary to cut the limit
        // in half to avoid having the limit drift upwards when the RTT is probed during heavy load.
        // To avoid decreasing the limit too much we don't allow it to go lower than the queueSize.
        if (probeInterval != DISABLED && resetRttCounter-- <= 0) {
            resetRttCounter = nextProbeCountdown();
            
            estimatedLimit = Math.max(minLimit, queueSize);
            rttNoLoadMeasurement.reset();
            lastRtt = 0;
            LOG.debug("Probe MinRTT limit={}", getLimit());
            return (int)estimatedLimit;
        }
        
        final long rttNoLoad = rttNoLoadMeasurement.add(rtt).longValue();
        minRttSampleListener.addLongSample(rttNoLoad);
        
        // Rtt could be higher than rtt_noload because of smoothing rtt noload updates
        // so set to 1.0 to indicate no queuing.  Otherwise calculate the slope and don't
        // allow it to be reduced by more than half to avoid aggressive load-sheding due to 
        // outliers.
        final double gradient = Math.max(0.5, Math.min(1.0, rttTolerance * rttNoLoad / rtt));
        
        double newLimit;
        // Reduce the limit aggressively if there was a drop
        if (didDrop) {
            newLimit = estimatedLimit * backoffRatio;
        // Don't grow the limit if we are app limited
        } else if (inflight < estimatedLimit / 2) {
            return (int)estimatedLimit;
        } else {
            newLimit = estimatedLimit * gradient + queueSize;
        }

        if (newLimit < estimatedLimit) {
            newLimit = Math.max(minLimit, estimatedLimit * (1-smoothing) + smoothing*(newLimit));
        }
        newLimit = Math.max(queueSize, Math.min(maxLimit, newLimit));
        
        if ((int)newLimit != (int)estimatedLimit  && LOG.isDebugEnabled()) {
            LOG.debug("New limit={} minRtt={} ms winRtt={} ms queueSize={} gradient={} resetCounter={}", 
                    (int)newLimit, 
                    TimeUnit.NANOSECONDS.toMicros(rttNoLoad)/1000.0, 
                    TimeUnit.NANOSECONDS.toMicros(rtt)/1000.0,
                    queueSize,
                    gradient,
                    resetRttCounter);
        }

        estimatedLimit = newLimit;
        return (int)estimatedLimit;
    }

    public long getLastRtt(TimeUnit units) {
        return units.convert(lastRtt, TimeUnit.NANOSECONDS);
    }

    public long getRttNoLoad(TimeUnit units) {
        return units.convert(rttNoLoadMeasurement.get().longValue(), TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "GradientLimit [limit=" + (int)estimatedLimit + 
                ", rtt_noload=" + TimeUnit.MICROSECONDS.toMillis(rttNoLoadMeasurement.get().longValue()) / 1000.0+
                " ms]";
    }
}
