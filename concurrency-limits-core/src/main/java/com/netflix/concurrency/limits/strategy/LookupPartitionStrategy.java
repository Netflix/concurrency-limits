package com.netflix.concurrency.limits.strategy;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.MetricRegistry.SampleListener;
import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;

/**
 * Strategy for partitioning the limiter by named groups where the allocation of 
 * group to percentage is provided up front. 
 * @param <T>
 */
public class LookupPartitionStrategy<T> implements Strategy<T> {
    private static final String PARTITION_TAG_NAME = "partition";
    
    public static class Builder<T> {
        private final Function<T, String> lookup;
        private final Map<String, Partition> partitions = new LinkedHashMap<>();
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        
        protected Builder(Function<T, String> lookup) {
            this.lookup = lookup;
        }

        public Builder<T> metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }
        
        public Builder<T> assign(String group, Double percent) {
            partitions.put(group, new Partition(group, percent));
            return this;
        }
        
        public LookupPartitionStrategy<T> build() {
            return new LookupPartitionStrategy<T>(this);
        }

        public boolean hasPartitions() {
            return !partitions.isEmpty();
        }
    }
    
    public static <T> Builder<T> newBuilder(Function<T, String> lookup) {
        return new Builder<T>(lookup);
    }

    private final Map<String, Partition> partitions;
    private final Partition unknownPartition;
    private final Function<T, String> lookup;
    private int busy = 0;
    private int limit = 0;
    
    private LookupPartitionStrategy(Builder<T> builder) {
        Preconditions.checkArgument(!builder.partitions.isEmpty(), "No partitions specified");
        Preconditions.checkArgument(builder.partitions.values().stream().map(Partition::getPercent).reduce(0.0, Double::sum) <= 1.0, 
                "Sum of percentages must be <= 1.0");

        this.partitions = new HashMap<>(builder.partitions);
        this.partitions.forEach((name, partition) -> partition.createMetrics(builder.registry));
        
        this.unknownPartition = new Partition("unknown", 0.0);
        this.unknownPartition.createMetrics(builder.registry);
        
        this.lookup = builder.lookup;
        
        builder.registry.registerGauge(MetricIds.LIMIT_GUAGE_NAME, this::getLimit);
    }
    
    @Override
    public synchronized Token tryAcquire(T type) {
        final Partition partition = partitions.getOrDefault(lookup.apply(type), this.unknownPartition);
        
        if (busy >= limit && partition.isLimitExceeded()) {
            return Token.newNotAcquired(busy);
        }
        busy++;
        partition.acquire();
        return Token.newAcquired(busy, () -> releasePartition(partition));
    }

    private synchronized void releasePartition(Partition partition) {
        busy--;
        partition.release();
    }

    @Override
    public synchronized void setLimit(int newLimit) { 
        if (this.limit != newLimit) {
            this.limit = newLimit;
            partitions.forEach((name, partition) -> partition.updateLimit(newLimit));
        }
    }

    public synchronized int getLimit() {
        return limit;
    }

    private static class Partition {
        private final double percent;
        private final String name;
        private SampleListener busyDistribution;
        private int limit;
        private int busy;
        
        public Partition(String name, double pct) {
            this.name = name;
            this.percent = pct;
        }
        
        public void createMetrics(MetricRegistry registry) {
            this.busyDistribution = registry.registerDistribution(MetricIds.INFLIGHT_GUAGE_NAME, PARTITION_TAG_NAME, name);
            registry.registerGauge(MetricIds.PARTITION_LIMIT_GUAGE_NAME, this::getLimit, PARTITION_TAG_NAME, name);
        }
        
        public void updateLimit(int totalLimit) {
            // Calculate this bin's limit while rounding up and ensuring the value
            // is at least 1.  With this technique the sum of bin limits may end up being 
            // higher than the concurrency limit.
            this.limit = (int)Math.max(1, Math.ceil(totalLimit * percent));
        }

        public boolean isLimitExceeded() {
            return busy >= limit;
        }

        public void acquire() {
            busy++;
            busyDistribution.addSample(busy);
        }
        
        public void release() {
            busy--;
        }

        public int getLimit() {
            return limit;
        }
        
        public double getPercent() {
            return percent;
        }
        
        @Override
        public String toString() {
            return "Partition [pct=" + percent + ", limit=" + limit + ", busy=" + busy + "]";
        }
    }

    synchronized int getBinBusyCount(String key) {
        return Optional.ofNullable(partitions.get(key))
                .orElseThrow(() -> new IllegalArgumentException("Invalid group " + key))
                .busy;
    }

    synchronized int getBinLimit(String key) {
        return Optional.ofNullable(partitions.get(key))
                .orElseThrow(() -> new IllegalArgumentException("Invalid group " + key))
                .limit;
    }
    
    synchronized int getBusyCount() {
        return busy;
    }

    @Override
    public String toString() {
        return "LookupPartitionedStrategy [partitions=" + partitions + ", unknownPartition=" + unknownPartition
                + ", limit=" + limit + "]";
    }
}
