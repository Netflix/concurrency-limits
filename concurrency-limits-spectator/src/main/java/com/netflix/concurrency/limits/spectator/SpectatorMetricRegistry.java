package com.netflix.concurrency.limits.spectator;

import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;

import java.util.function.Supplier;

public final class SpectatorMetricRegistry implements MetricRegistry {
    private final Registry registry;
    private final String baseId;
    
    public SpectatorMetricRegistry(String baseId, Registry registry) {
        this.registry = registry;
        this.baseId = baseId;
    }
    
    public void registerGuage(String id, Supplier<Number> supplier) {
        PolledMeter.using(registry)
            .withName(this.baseId + "." + id)
            .monitorValue(this, o -> supplier.get().doubleValue());
    }

    @Override
    public SampleListener registerDistribution(String id, String... tagNameValuePairs) {
        DistributionSummary summary = registry.distributionSummary(this.baseId + "." + id, tagNameValuePairs);
        return value -> summary.record(value.longValue());
    }

    @Override
    public void registerGuage(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        PolledMeter.using(registry)
            .withName(this.baseId + "." + id)
            .withTags(tagNameValuePairs)
            .monitorValue(this, o -> supplier.get().doubleValue());
    }
}
