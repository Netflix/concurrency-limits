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
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.limit.measurement.ExpAvgMeasurement;
import com.netflix.concurrency.limits.limit.measurement.Measurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Concurrency limit algorithm that adjust the limits based on the gradient of change in the 
 * samples minimum RTT and absolute minimum RTT allowing for a queue of square root of the 
 * current limit.  Why square root?  Because it's better than a fixed queue size that becomes too
 * small for large limits but still prevents the limit from growing too much by slowing down
 * growth as the limit grows.
 */
public final class Gradient2Limit extends AbstractLimit {
    private static final int DISABLED = -1;

    private static final Logger LOG = LoggerFactory.getLogger(Gradient2Limit.class);

    public static class Builder {
        private int initialLimit = 4;
        private int minLimit = 4;
        private int maxConcurrency = 1000;

        private double smoothing = 0.2;
        private Function<Integer, Integer> queueSize = concurrency -> 4;
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private int shortWindow = 10;
        private int longWindow = 100;
        private int driftMultiplier = 5;

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
         * Maximum multiple of the fast window after which we need to reset the limiter
         * @param multiplier
         * @return
         */
        public Builder driftMultiplier(int multiplier) {
            this.driftMultiplier = multiplier;
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

        public Builder shortWindow(int n) {
            this.shortWindow = n;
            return this;
        }

        public Builder longWindow(int n) {
            this.longWindow = n;
            return this;
        }

        public Gradient2Limit build() {
            return new Gradient2Limit(this);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Gradient2Limit newDefault() {
        return newBuilder().build();
    }

    /**
     * Estimated concurrency limit based on our algorithm
     */
    private volatile double estimatedLimit;

    /**
     * Tracks a measurement of the short time, and more volatile, RTT meant to represent the current system latency
     */
    private final Measurement shortRtt;

    /**
     * Tracks a measurement of the long term, less volatile, RTT meant to represent the baseline latency.  When the system
     * is under load this number is expect to trend higher.
     */
    private final Measurement longRtt;

    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit;

    private final int minLimit;

    private final Function<Integer, Integer> queueSize;

    private final double smoothing;

    private final SampleListener longRttSampleListener;

    private final SampleListener shortRttSampleListener;

    private final SampleListener queueSizeSampleListener;

    private final int maxDriftIntervals;

    private int intervalsAbove = 0;

    private Gradient2Limit(Builder builder) {
        super(builder.initialLimit);

        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.minLimit = builder.minLimit;
        this.queueSize = builder.queueSize;
        this.smoothing = builder.smoothing;
        this.shortRtt = new ExpAvgMeasurement(builder.shortWindow,10);
        this.longRtt = new ExpAvgMeasurement(builder.longWindow,10);
        this.maxDriftIntervals = builder.shortWindow * builder.driftMultiplier;

        this.longRttSampleListener = builder.registry.registerDistribution(MetricIds.MIN_RTT_NAME);
        this.shortRttSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_MIN_RTT_NAME);
        this.queueSizeSampleListener = builder.registry.registerDistribution(MetricIds.WINDOW_QUEUE_SIZE_NAME);
    }

    @Override
    public int _update(final long startTime, final long rtt, final int inflight, final boolean didDrop) {
        final double queueSize = this.queueSize.apply((int)this.estimatedLimit);

        final double shortRtt = this.shortRtt.add(rtt).doubleValue();
        final double longRtt = this.longRtt.add(rtt).doubleValue();

        // Under steady state we expect the short and long term RTT to whipsaw.  We can identify that a system is under
        // long term load when there is no crossover detected for a certain number of internals, normally a multiple of
        // the short term RTT window.  Since both short and long term RTT trend higher this state results in the limit
        // slowly trending upwards, increasing the queue and latency.  To mitigate this we drop both the limit and
        // long term latency value to effectively probe for less queueing and better latency.
        if (shortRtt > longRtt) {
            intervalsAbove++;
            if (intervalsAbove > maxDriftIntervals) {
                intervalsAbove = 0;
                int newLimit = (int) Math.max(minLimit, queueSize);
                this.longRtt.reset();
                estimatedLimit = newLimit;
                return (int) estimatedLimit;
            }
        } else {
            intervalsAbove = 0;
        }

        shortRttSampleListener.addSample(shortRtt);
        longRttSampleListener.addSample(longRtt);
        queueSizeSampleListener.addSample(queueSize);

        // Rtt could be higher than rtt_noload because of smoothing rtt noload updates
        // so set to 1.0 to indicate no queuing.  Otherwise calculate the slope and don't
        // allow it to be reduced by more than half to avoid aggressive load-sheding due to 
        // outliers.
        final double gradient = Math.max(0.5, Math.min(1.0, longRtt / shortRtt));
        
        double newLimit;
        // Don't grow the limit if we are app limited
        if (inflight < estimatedLimit / 2) {
            return (int) estimatedLimit;
        }

        newLimit = estimatedLimit * gradient + queueSize;
        if (newLimit < estimatedLimit) {
            newLimit = Math.max(minLimit, estimatedLimit * (1-smoothing) + smoothing*(newLimit));
        }
        newLimit = Math.max(queueSize, Math.min(maxLimit, newLimit));

        if ((int)estimatedLimit != newLimit) {
            LOG.debug("New limit={} shortRtt={} ms longRtt={} ms queueSize={} gradient={}",
                    (int) newLimit,
                    getShortRtt(TimeUnit.MICROSECONDS) / 1000.0,
                    getLongRtt(TimeUnit.MICROSECONDS) / 1000.0,
                    queueSize,
                    gradient);
        }

        estimatedLimit = newLimit;

        return (int)estimatedLimit;
    }

    public long getShortRtt(TimeUnit units) {
        return units.convert(shortRtt.get().longValue(), TimeUnit.NANOSECONDS);
    }

    public long getLongRtt(TimeUnit units) {
        return units.convert(longRtt.get().longValue(), TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "GradientLimit [limit=" + (int)estimatedLimit + "]";
    }
}
