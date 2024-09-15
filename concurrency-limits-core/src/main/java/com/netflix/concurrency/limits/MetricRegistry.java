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
package com.netflix.concurrency.limits;

import java.util.function.Supplier;

/**
 * Simple abstraction for tracking metrics in the limiters.
 * 
 */
public interface MetricRegistry {
    /**
     * Listener to receive samples for a distribution
     */
    interface SampleListener {
        void addSample(Number value);

        default void addLongSample(long value) {
            addSample(value);
        }

        default void addDoubleSample(double value) {
            addSample(value);
        }
    }

    interface Counter {
        void increment();
    }

    /**
     * @deprecated Use {@link #distribution(String, String...)}
     */
    @Deprecated
    default SampleListener registerDistribution(String id, String... tagNameValuePairs) {
        throw new UnsupportedOperationException("registerDistribution is deprecated");
    }

    /**
     * Register a sample distribution.  Samples are added to the distribution via the returned
     * {@link SampleListener}.  Will reuse an existing {@link SampleListener} if the distribution already
     * exists.
     *
     * @param id
     * @param tagNameValuePairs Pairs of tag name and tag value.  Number of parameters must be a multiple of 2.
     * @return SampleListener for the caller to add samples
     */
    default SampleListener distribution(String id, String... tagNameValuePairs) {
        return registerDistribution(id, tagNameValuePairs);
    }

    /**
     * @deprecated Use {@link #gauge(String, Supplier, String...)}
     */
    @Deprecated
    default void registerGauge(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        throw new UnsupportedOperationException("registerGauge is deprecated");
    }

    /**
     * Register a gauge using the provided supplier.  The supplier will be polled whenever the guage
     * value is flushed by the registry.
     *
     * @param id
     * @param tagNameValuePairs Pairs of tag name and tag value.  Number of parameters must be a multiple of 2.
     * @param supplier
     */
    default void gauge(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        registerGauge(id, supplier, tagNameValuePairs);
    };

    /**
     * Create a counter that will be increment when an event occurs.  Counters normally translate in an action
     * per second metric.
     *
     * @param id
     * @param tagNameValuePairs
     */
    default Counter counter(String id, String... tagNameValuePairs) {
        return () -> {};
    }

    /**
     * @deprecated Call MetricRegistry#registerGauge
     */
    @Deprecated
    default void registerGuage(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        gauge(id, supplier, tagNameValuePairs);
    }
}
