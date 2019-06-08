package com.netflix.concurrency.limits.spectator;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.patterns.PolledMeter;

public class SpectatorMetricRegistryTest {
    @Test
    public void testGuage() {
        DefaultRegistry registry = new DefaultRegistry();
        SpectatorMetricRegistry metricRegistry = new SpectatorMetricRegistry(registry, registry.createId("foo"));
        
        metricRegistry.gauge("bar", () -> 10);
        
        PolledMeter.update(registry);
        
        Assert.assertEquals(10.0, registry.gauge(registry.createId("foo.bar")).value(), 0);
    }
    
    @Test
    public void testUnregister() {
        DefaultRegistry registry = new DefaultRegistry();
        SpectatorMetricRegistry metricRegistry = new SpectatorMetricRegistry(registry, registry.createId("foo"));
        
        metricRegistry.gauge("bar", () -> 10);
        metricRegistry.gauge("bar", () -> 20);
        
        PolledMeter.update(registry);
        
        Assert.assertEquals(20.0, registry.gauge(registry.createId("foo.bar")).value(), 0);
        
    }
}
