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
    
    public void guage(String id, Supplier<Number> supplier) {
        PolledMeter.using(registry)
            .withName(this.baseId + "." + id)
            .monitorValue(this, o -> supplier.get().doubleValue());
    }

    @Override
    public Metric metric(String id) {
        DistributionSummary summary = registry.distributionSummary(this.baseId + "." + id);
        return value -> summary.record(value.longValue());
    }

    @Override
    public Metric metric(String id, String tagName, String valueName) {
        DistributionSummary summary = registry.distributionSummary(registry.createId(this.baseId + "." + id, tagName, valueName));
        return value -> summary.record(value.longValue());
    }

    @Override
    public void guage(String id, String tagName, String tagValue, Supplier<Number> supplier) {
        PolledMeter.using(registry)
            .withName(this.baseId + "." + id)
            .withTag(tagName, tagValue)
            .monitorValue(this, o -> supplier.get().doubleValue());
    }
}
