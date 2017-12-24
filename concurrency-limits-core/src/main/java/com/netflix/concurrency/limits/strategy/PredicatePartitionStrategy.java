package com.netflix.concurrency.limits.strategy;

import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.MetricRegistry.Metric;
import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Concurrency limiter that guarantees a certain percentage of the limit to specific callers
 * while allowing callers to borrow from underutilized callers.
 * 
 * Callers are identified by their index into an array of percentages passed in during initialization.
 * A percentage of 0.0 means that a caller may only use excess capacity and can be completely 
 * starved when the limit is reached.  A percentage of 1.0 means that the caller
 * is guaranteed to get the entire limit.
 * 
 * grpc.server.call.inflight (group=[a, b, c])
 * grpc.server.call.limit (group=[a,b,c])
 */
public final class PredicatePartitionStrategy<T> implements Strategy<T> {
    private static final String PARTITION_TAG_NAME = "partition";
    
    public static class Builder<T> {
        private final List<Partition<T>> partitions = new ArrayList<>();
        private MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        
        public Builder<T> metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }
        
        public Builder<T> add(String name, Double pct, Predicate<T> predicate) {
            partitions.add(new Partition<T>(name, pct, predicate));
            return this;
        }
        
        public PredicatePartitionStrategy<T> build() {
            return new PredicatePartitionStrategy<T>(this);
        }

        public boolean hasPartitions() {
            return !partitions.isEmpty();
        }
    }
    
    public static <T> Builder<T> newBuilder() {
        return new Builder<T>();
    }

    private final List<Partition<T>> partitions;
    private int busy = 0;
    private int limit = 0;
    
    private PredicatePartitionStrategy(Builder<T> builder) {
        Preconditions.checkArgument(builder.partitions.stream().map(Partition::getPercent).reduce(0.0, Double::sum) <= 1.0, 
                "Sum of percentages must be <= 1.0");

        this.partitions = new ArrayList<>(builder.partitions);
        this.partitions.forEach(partition -> partition.createMetrics(builder.registry));
        
        builder.registry.guage(MetricIds.LIMIT_METRIC_ID, this::getLimit);
    }
    
    @Override
    public synchronized Optional<Runnable> tryAcquire(T type) {
        for (Partition<T> partition : partitions) {
            if (partition.predicate.test(type)) {
                if (busy >= limit && partition.isLimitExceeded()) {
                    return Optional.empty();
                }
                busy++;
                partition.acquire();
                return Optional.of(() -> release(partition));
            }
        }
        return Optional.empty();
    }
    
    private synchronized void release(Partition<T> partition) {
        busy--;
        partition.release();
    }

    @Override
    public synchronized void setLimit(int newLimit) { 
        if (this.limit != newLimit) {
            this.limit = newLimit;
            partitions.forEach(partition -> partition.updateLimit(newLimit));
        }
    }
    
    public synchronized int getLimit() {
        return limit;
    }

    private static class Partition<T> {
        private final double percent;
        private final Predicate<T> predicate;
        private final String name;
        private Metric metric;
        private int limit;
        private int busy;
        
        public Partition(String name, double pct, Predicate<T> predicate) {
            this.name = name;
            this.percent = pct;
            this.predicate = predicate;
        }
        
        public void createMetrics(MetricRegistry registry) {
            this.metric = registry.metric(MetricIds.INFLIGHT_METRIC_ID, PARTITION_TAG_NAME, name);
            registry.guage(MetricIds.PARTITION_LIMIT_METRIC_ID, PARTITION_TAG_NAME, name, this::getLimit);
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
            metric.add(busy);
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

    synchronized int getBinBusyCount(int index) {
        Preconditions.checkArgument(index >= 0 && index < partitions.size(), "Invalid bin index " + index);
        return partitions.get(index).busy;
    }

    synchronized int getBinLimit(int index) {
        Preconditions.checkArgument(index >= 0 && index < partitions.size(), "Invalid bin index " + index);
        return partitions.get(index).limit;
    }
    
    synchronized public int getBusyCount() {
        return busy;
    }
    
    @Override
    public String toString() {
        final int maxLen = 10;
        return "PercentageStrategy [" + partitions.subList(0, Math.min(partitions.size(), maxLen)) + "]";
    }
}
