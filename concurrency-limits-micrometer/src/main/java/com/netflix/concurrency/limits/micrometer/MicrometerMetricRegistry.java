/**
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.concurrency.limits.micrometer;

import com.netflix.concurrency.limits.MetricRegistry;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.function.Supplier;

/**
 * A Micrometer-based implementation of {@link MetricRegistry}.
 * <p>
 * All meters created by this registry will be prefixed with {@code netflix.concurrency.limits.}.
 */
public class MicrometerMetricRegistry implements MetricRegistry {

    private static final String METRIC_NAME_PREFIX = "netflix.concurrency.limits.";

    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new {@link MicrometerMetricRegistry} with the given {@link MeterRegistry}.
     * @param meterRegistry the Micrometer meter registry to use
     */
    public MicrometerMetricRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public SampleListener distribution(String id, String... tagNameValuePairs) {
        final DistributionSummary summary =
                DistributionSummary.builder(METRIC_NAME_PREFIX + id)
                        .tags(tagNameValuePairs)
                        .register(meterRegistry);

        return value -> summary.record(value.doubleValue());
    }

    @Override
    public void gauge(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        Gauge.builder(METRIC_NAME_PREFIX + id, supplier)
                .tags(tagNameValuePairs)
                .register(meterRegistry);
    }

    @Override
    public Counter counter(String id, String... tagNameValuePairs) {
        return meterRegistry.counter(METRIC_NAME_PREFIX + id, tagNameValuePairs)::increment;
    }
}
