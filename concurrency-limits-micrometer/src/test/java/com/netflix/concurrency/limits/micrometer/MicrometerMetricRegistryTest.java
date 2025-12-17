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
package com.netflix.concurrency.limits.micrometer;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.MetricRegistry.Counter;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MicrometerMetricRegistryTest {

    private SimpleMeterRegistry meterRegistry;

    private MicrometerMetricRegistry metricRegistry;

    @Before
    public void setup() {
        meterRegistry = new SimpleMeterRegistry();
        metricRegistry = new MicrometerMetricRegistry(meterRegistry);
    }

    @Test
    public void testDistribution() {
        SampleListener listener = metricRegistry.distribution("distribution", "foo", "bar");
        listener.addDoubleSample(40);
        listener.addDoubleSample(2);

        double sum = meterRegistry.get("netflix.concurrency.limits.distribution")
                .tag("foo", "bar")
                .summary()
                .totalAmount();

        Assert.assertEquals(42, sum, 0);
    }

    @Test
    public void testGauge() {
        final AtomicLong value = new AtomicLong(42);
        metricRegistry.gauge("gauge", () -> value, "foo", "bar");

        double gaugeValue = meterRegistry.get("netflix.concurrency.limits.gauge")
                .tag("foo", "bar")
                .gauge()
                .value();

        Assert.assertEquals(42, gaugeValue, 0);

        value.set(43);

        gaugeValue = meterRegistry.get("netflix.concurrency.limits.gauge")
                .tag("foo", "bar")
                .gauge()
                .value();

        Assert.assertEquals(43, gaugeValue, 0);
    }

    @Test
    public void testCounter() {
        Counter counter = metricRegistry.counter("counter", "foo", "bar");
        counter.increment();
        counter.increment();

        double count = meterRegistry.get("netflix.concurrency.limits.counter")
                .tag("foo", "bar")
                .counter()
                .count();

        Assert.assertEquals(2, count, 0);
    }

    @Test
    public void testMeterIdTags() {
        Limit limit = VegasLimit.newBuilder().named("testVegas").metricRegistry(metricRegistry).build();
        SimpleLimiter.newBuilder()
                .limit(limit)
                .metricRegistry(metricRegistry)
                .named("testSimple")
                .build();

        List<String> actualTagIds = meterRegistry.getMeters().stream()
                .map(Meter::getId)
                .map(id -> id.getTag("id"))
                .filter(Objects::nonNull)
                .sorted()
                .collect(Collectors.toList());

        List<String> expectedTagIds = new ArrayList<>();
        expectedTagIds.add("testSimple");
        expectedTagIds.add("testSimple");
        expectedTagIds.add("testSimple");
        expectedTagIds.add("testSimple");
        expectedTagIds.add("testSimple");
        expectedTagIds.add("testSimple");
        expectedTagIds.add("testVegas");

        Assert.assertEquals(expectedTagIds, actualTagIds);
    }

}
