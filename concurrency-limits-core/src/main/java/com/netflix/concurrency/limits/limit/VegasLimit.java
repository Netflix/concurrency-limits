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
import com.netflix.concurrency.limits.limit.functions.Log10RootIntFunction;
import com.netflix.concurrency.limits.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

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
public class VegasLimit extends AbstractLimit {
    private static final Logger LOG = LoggerFactory.getLogger(VegasLimit.class);
    
    private static final IntUnaryOperator LOG10 = Log10RootIntFunction.create(0);

    public static class Builder extends AbstractLimit.Builder<Builder>{
        private int maxConcurrency = 1000;
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        private double smoothing = 1.0;
        
        private IntUnaryOperator alphaFunc = (limit) -> 3 * LOG10.applyAsInt(limit);
        private IntUnaryOperator betaFunc = (limit) -> 6 * LOG10.applyAsInt(limit);
        private IntUnaryOperator thresholdFunc = LOG10;
        private DoubleUnaryOperator increaseFunc = (limit) -> limit + LOG10.applyAsInt((int) limit);
        private DoubleUnaryOperator decreaseFunc = (limit) -> limit - LOG10.applyAsInt((int) limit);
        private int probeMultiplier = 30;
        
        private Builder() {
            super(20);
        }
        
        /**
         * The limiter will probe for a new noload RTT every probeMultiplier * current limit
         * iterations.  Default value is 30.  
         * @param probeMultiplier 
         * @return Chainable builder
         */
        public Builder probeMultiplier(int probeMultiplier) {
            this.probeMultiplier = probeMultiplier;
            return this;
        }
        
        public Builder alpha(int alpha) {
            this.alphaFunc = (ignore) -> alpha;
            return this;
        }

        /**
         * @deprecated use {@link #thresholdFunction(IntUnaryOperator)}
         */
        @Deprecated
        public Builder threshold(Function<Integer, Integer> threshold) {
            this.thresholdFunc = threshold::apply;
            return this;
        }

        public Builder thresholdFunction(IntUnaryOperator threshold) {
            this.thresholdFunc = threshold;
            return this;
        }

        /**
         * @deprecated use {@link #alphaFunction(IntUnaryOperator)}
         */
        @Deprecated
        public Builder alpha(Function<Integer, Integer> alpha) {
            this.alphaFunc = alpha::apply;
            return this;
        }

        public Builder alphaFunction(IntUnaryOperator alpha) {
            this.alphaFunc = alpha;
            return this;
        }
        
        public Builder beta(int beta) {
            this.betaFunc = (ignore) -> beta;
            return this;
        }

        /**
         * @deprecated use {@link #betaFunction(IntUnaryOperator)}
         */
        @Deprecated
        public Builder beta(Function<Integer, Integer> beta) {
            this.betaFunc = beta::apply;
            return this;
        }

        public Builder betaFunction(IntUnaryOperator beta) {
            this.betaFunc = beta;
            return this;
        }

        /**
         * @deprecated use {@link #increaseFunction(DoubleUnaryOperator)}
         */
        @Deprecated
        public Builder increase(Function<Double, Double> increase) {
            this.increaseFunc = increase::apply;
            return this;
        }

        public Builder increaseFunction(DoubleUnaryOperator increase) {
            this.increaseFunc = increase;
            return this;
        }

        /**
         * @deprecated use {@link #decreaseFunction(DoubleUnaryOperator)}
         */
        @Deprecated
        public Builder decrease(Function<Double, Double> decrease) {
            this.decreaseFunc = decrease::apply;
            return this;
        }

        public Builder decreaseFunction(DoubleUnaryOperator decrease) {
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

        @Override
        protected Builder self() {
            return this;
        }

        public VegasLimit build() {
            if (initialLimit > maxConcurrency) {
                LOG.warn("Initial limit {} exceeded maximum limit {}", initialLimit, maxConcurrency);
            }
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
    private volatile double estimatedLimit;
    
    private volatile long rtt_noload = 0;
    
    /**
     * Maximum allowed limit providing an upper bound failsafe
     */
    private final int maxLimit; 
    
    private final double smoothing;
    private final IntUnaryOperator alphaFunc;
    private final IntUnaryOperator betaFunc;
    private final IntUnaryOperator thresholdFunc;
    private final DoubleUnaryOperator increaseFunc;
    private final DoubleUnaryOperator decreaseFunc;
    private final SampleListener rttSampleListener;
    private final int probeMultiplier;
    private int probeCount = 0;
    private double probeJitter;

    private VegasLimit(Builder builder) {
        super(builder);
        this.estimatedLimit = builder.initialLimit;
        this.maxLimit = builder.maxConcurrency;
        this.alphaFunc = builder.alphaFunc;
        this.betaFunc = builder.betaFunc;
        this.increaseFunc = builder.increaseFunc;
        this.decreaseFunc = builder.decreaseFunc;
        this.thresholdFunc = builder.thresholdFunc;
        this.smoothing = builder.smoothing;
        this.probeMultiplier = builder.probeMultiplier;

        resetProbeJitter();

        this.rttSampleListener = builder.registry.distribution(MetricIds.MIN_RTT_NAME, Tags.ID_NAME, builder.name);
    }

    private void resetProbeJitter() {
        probeJitter = ThreadLocalRandom.current().nextDouble(0.5, 1);
    }

    private boolean shouldProbe() {
        return probeJitter * probeMultiplier * estimatedLimit <= probeCount;
    }

    @Override
    protected int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        if (rtt <= 0) {
            throw new IllegalArgumentException("rtt must be >0 but got " + rtt);
        }

        probeCount++;
        if (shouldProbe()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Probe MinRTT {}", TimeUnit.NANOSECONDS.toMicros(rtt) / 1000.0);
            }
            resetProbeJitter();
            probeCount = 0;
            rtt_noload = rtt;
            return (int) estimatedLimit;
        }

        long rtt_noload = this.rtt_noload;
        if (rtt_noload == 0 || rtt < rtt_noload) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("New MinRTT {}", TimeUnit.NANOSECONDS.toMicros(rtt) / 1000.0);
            }
            this.rtt_noload = rtt;
            return (int) estimatedLimit;
        }

        rttSampleListener.addLongSample(rtt_noload);

        return updateEstimatedLimit(rtt, rtt_noload, inflight, didDrop);
    }

    private int updateEstimatedLimit(long rtt, long rtt_noload, int inflight, boolean didDrop) {
        double estimatedLimit = this.estimatedLimit;
        final int queueSize = (int) Math.ceil(estimatedLimit * (1 - (double) rtt_noload / rtt));

        double newLimit;
        // Treat any drop (i.e timeout) as needing to reduce the limit
        if (didDrop) {
            newLimit = decreaseFunc.applyAsDouble(estimatedLimit);
        // Prevent upward drift if not close to the limit
        } else if (inflight * 2 < estimatedLimit) {
            return (int) estimatedLimit;
        } else {
            int alpha = alphaFunc.applyAsInt((int) estimatedLimit);
            int beta = betaFunc.applyAsInt((int) estimatedLimit);
            int threshold = thresholdFunc.applyAsInt((int) estimatedLimit);

            // Aggressive increase when no queuing
            if (queueSize <= threshold) {
                newLimit = estimatedLimit + beta;
            // Increase the limit if queue is still manageable
            } else if (queueSize < alpha) {
                newLimit = increaseFunc.applyAsDouble(estimatedLimit);
            // Detecting latency so decrease
            } else if (queueSize > beta) {
                newLimit = decreaseFunc.applyAsDouble(estimatedLimit);
            // We're within he sweet spot so nothing to do
            } else {
                return (int) estimatedLimit;
            }
        }

        newLimit = Math.max(1, Math.min(maxLimit, newLimit));
        newLimit = (1 - smoothing) * estimatedLimit + smoothing * newLimit;
        if ((int) newLimit != (int) estimatedLimit && LOG.isDebugEnabled()) {
            LOG.debug("New limit={} minRtt={} ms winRtt={} ms queueSize={}",
                    (int) newLimit,
                    TimeUnit.NANOSECONDS.toMicros(rtt_noload) / 1000.0,
                    TimeUnit.NANOSECONDS.toMicros(rtt) / 1000.0,
                    queueSize);
        }
        this.estimatedLimit = newLimit;
        return (int) newLimit;
    }

    @Override
    public String toString() {
        return "VegasLimit [limit=" + getLimit() + 
                ", rtt_noload=" + TimeUnit.NANOSECONDS.toMicros(rtt_noload) / 1000.0 +
                " ms]";
    }
}
