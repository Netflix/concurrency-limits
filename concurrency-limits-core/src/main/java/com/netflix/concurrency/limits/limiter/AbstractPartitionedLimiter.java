package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public abstract class AbstractPartitionedLimiter<ContextT> extends AbstractLimiter<ContextT> {
    private static final String PARTITION_TAG_NAME = "partition";

    public abstract static class Builder<BuilderT extends AbstractLimiter.Builder<BuilderT, ContextT>, ContextT> extends AbstractLimiter.Builder<BuilderT, ContextT> {
        private List<Function<ContextT, String>> partitionResolvers = new ArrayList<>();
        private final Map<String, Partition> partitions = new LinkedHashMap<>();

        public BuilderT partitionResolver(Function<ContextT, String> contextToPartition) {
            this.partitionResolvers.add(contextToPartition);
            return self();
        }

        public BuilderT partition(String name, double percent) {
            partitions.put(name, new Partition(name, percent));
            return self();
        }

        protected boolean hasPartitions() {
            return !partitions.isEmpty();
        }

        public Limiter<ContextT> build() {
            return (this.hasPartitions() && !partitionResolvers.isEmpty())
                    ? new AbstractPartitionedLimiter<ContextT>(this) {}
                    : new SimpleLimiter<ContextT>(this);
        }
    }

    static class Partition {
        private final double percent;
        private final String name;
        private int limit;
        private int busy;
        private MetricRegistry.SampleListener inflightDistribution;

        Partition(String name, double pct) {
            this.name = name;
            this.percent = pct;
        }

        void updateLimit(int totalLimit) {
            // Calculate this bin's limit while rounding up and ensuring the value
            // is at least 1.  With this technique the sum of bin limits may end up being
            // higher than the concurrency limit.
            this.limit = (int)Math.max(1, Math.ceil(totalLimit * percent));
        }

        boolean isLimitExceeded() {
            return busy >= limit;
        }

        void acquire() {
            busy++;
        }

        void release() {
            busy--;
        }

        int getLimit() {
            return limit;
        }

        public int getInflight() {
            return busy;
        }

        double getPercent() {
            return percent;
        }

        void createMetrics(MetricRegistry registry) {
            this.inflightDistribution = registry.registerDistribution(MetricIds.INFLIGHT_GUAGE_NAME, PARTITION_TAG_NAME, name);
            registry.registerGauge(MetricIds.PARTITION_LIMIT_GUAGE_NAME, this::getLimit, PARTITION_TAG_NAME, name);
        }

        @Override
        public String toString() {
            return "Partition [pct=" + percent + ", limit=" + limit + ", busy=" + busy + "]";
        }
    }

    private final Map<String, Partition> partitions;
    private final Partition unknownPartition;
    private final List<Function<ContextT, String>> partitionResolvers;

    public AbstractPartitionedLimiter(Builder<?, ContextT> builder) {
        super(builder);

        Preconditions.checkArgument(!builder.partitions.isEmpty(), "No partitions specified");
        Preconditions.checkArgument(builder.partitions.values().stream().map(Partition::getPercent).reduce(0.0, Double::sum) <= 1.0,
                "Sum of percentages must be <= 1.0");

        this.partitions = new HashMap<>(builder.partitions);
        this.partitions.forEach((name, partition) -> partition.createMetrics(builder.registry));

        this.unknownPartition = new Partition("unknown", 0.0);
        this.unknownPartition.createMetrics(builder.registry);

        this.partitionResolvers = builder.partitionResolvers;

        builder.registry.registerGauge(MetricIds.LIMIT_GUAGE_NAME, this::getLimit);

        onNewLimit(getLimit());
    }

    private Partition resolvePartition(ContextT context) {
        for (Function<ContextT, String> resolver : this.partitionResolvers) {
            String name = resolver.apply(context);
            if (name != null) {
                Partition partition = partitions.get(name);
                if (partition != null) {
                    return partition;
                }
            }
        }
        return unknownPartition;
    }

    @Override
    public synchronized Optional<Listener> acquire(ContextT context) {
        final Partition partition = resolvePartition(context);

        if (getInflight() >= getLimit() && partition.isLimitExceeded()) {
            return Optional.empty();
        }

        partition.acquire();
        final Listener listener = createListener();
        return Optional.of(new Listener() {
            @Override
            public void onSuccess() {
                listener.onSuccess();
                releasePartition(partition);
            }

            @Override
            public void onIgnore() {
                listener.onIgnore();
                releasePartition(partition);
            }

            @Override
            public void onDropped() {
                listener.onDropped();
                releasePartition(partition);
            }
        });
    }

    private synchronized void releasePartition(Partition partition) {
        partition.release();
    }

    @Override
    protected synchronized void onNewLimit(int newLimit) {
        partitions.forEach((name, partition) -> partition.updateLimit(newLimit));
    }

    Partition getPartition(String name) {
        return partitions.get(name);
    }
}
