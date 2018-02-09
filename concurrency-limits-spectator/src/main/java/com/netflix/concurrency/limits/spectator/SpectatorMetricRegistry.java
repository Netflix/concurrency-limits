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
