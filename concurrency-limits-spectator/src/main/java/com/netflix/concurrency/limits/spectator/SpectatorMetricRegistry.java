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
package com.netflix.concurrency.limits.spectator;

import java.util.function.Supplier;

import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;

public final class SpectatorMetricRegistry implements MetricRegistry {
    private final Registry registry;
    private final Id baseId;
    
    public SpectatorMetricRegistry(Registry registry, Id baseId) {
        this.registry = registry;
        this.baseId = baseId;
    }
    
    @Override
    public SampleListener registerDistribution(String id, String... tagNameValuePairs) {
        DistributionSummary summary = registry.distributionSummary(suffixBaseId(id).withTags(tagNameValuePairs));
        return value -> summary.record(value.longValue());
    }

    @Override
    public void registerGauge(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        Id metricId = suffixBaseId(id).withTags(tagNameValuePairs);
        PolledMeter.remove(registry, metricId);
        
        PolledMeter.using(registry)
            .withId(metricId)
            .monitorValue(supplier, ignore -> supplier.get().doubleValue());
    }
    
    private Id suffixBaseId(String suffix) {
        return registry.createId(this.baseId.name() + "." + suffix).withTags(this.baseId.tags());
    }

}
