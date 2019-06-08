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
package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public abstract class AbstractPartitionedLimiter<ContextT> extends AbstractLimiter<ContextT> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractPartitionedLimiter.class);
    private static final String PARTITION_TAG_NAME = "partition";

    public abstract static class Builder<BuilderT extends AbstractLimiter.Builder<BuilderT>, ContextT> extends AbstractLimiter.Builder<BuilderT> {
        private List<Function<ContextT, String>> partitionResolvers = new ArrayList<>();
        private final Map<String, Partition> partitions = new LinkedHashMap<>();
        private int maxDelayedThreads = 100;

        /**
         * Add a resolver from context to a partition name.  Multiple resolvers may be added and will be processed in
         * order with this first non-null value response used as the partition name.  If all resolvers return null then
         * the unknown partition is used
         * @param contextToPartition
         * @return Chainable builder
         */
        public BuilderT partitionResolver(Function<ContextT, String> contextToPartition) {
            this.partitionResolvers.add(contextToPartition);
            return self();
        }

        /**
         * Specify percentage of limit guarantees for a partition.  The total sum of partitions must add up to 100%
         * @param name
         * @param percent
         * @return Chainable builder
         */
        public BuilderT partition(String name, double percent) {
            Preconditions.checkArgument(name != null, "Partition name may not be null");
            Preconditions.checkArgument(percent >= 0.0 && percent <= 1.0, "Partition percentage must be in the range [0.0, 1.0]");
            partitions.computeIfAbsent(name, Partition::new).setPercent(percent);
            return self();
        }

        /**
         * Delay introduced in the form of a sleep to slow down the caller from the server side.  Because this can hold
         * off RPC threads it is not recommended to set a delay when using a direct executor in an event loop.  Also,
         * a max of 100 threads may be delayed before immediately returning
         * @param name
         * @param duration
         * @param units
         * @return Chainable builder
         */
        public BuilderT partitionRejectDelay(String name, long duration, TimeUnit units) {
            partitions.computeIfAbsent(name, Partition::new).setBackoffMillis(units.toMillis(duration));
            return self();
        }

        /**
         * Set the maximum number of threads that can be held up or delayed when rejecting excessive traffic for a partition.
         * The default value is 100.
         * @param maxDelayedThreads
         * @return Chainable builder
         */
        public BuilderT maxDelayedThreads(int maxDelayedThreads) {
            this.maxDelayedThreads = maxDelayedThreads;
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
        private final String name;

        private double percent = 0.0;
        private int limit = 0;
        private int busy = 0;
        private long backoffMillis = 0;
        private MetricRegistry.SampleListener inflightDistribution;

        Partition(String name) {
            this.name = name;
        }

        Partition setPercent(double percent) {
            this.percent = percent;
            return this;
        }

        Partition setBackoffMillis(long backoffMillis) {
            this.backoffMillis = backoffMillis;
            return this;
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
            inflightDistribution.addSample(busy);

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
            this.inflightDistribution = registry.distribution(MetricIds.INFLIGHT_NAME, PARTITION_TAG_NAME, name);
            registry.gauge(MetricIds.PARTITION_LIMIT_NAME, this::getLimit, PARTITION_TAG_NAME, name);
        }

        @Override
        public String toString() {
            return "Partition [pct=" + percent + ", limit=" + limit + ", busy=" + busy + "]";
        }
    }

    private final Map<String, Partition> partitions;
    private final Partition unknownPartition;
    private final List<Function<ContextT, String>> partitionResolvers;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicInteger delayedThreads = new AtomicInteger();
    private final int maxDelayedThreads;

    public AbstractPartitionedLimiter(Builder<?, ContextT> builder) {
        super(builder);

        Preconditions.checkArgument(!builder.partitions.isEmpty(), "No partitions specified");
        Preconditions.checkArgument(builder.partitions.values().stream().map(Partition::getPercent).reduce(0.0, Double::sum) <= 1.0,
                "Sum of percentages must be <= 1.0");

        this.partitions = new HashMap<>(builder.partitions);
        this.partitions.forEach((name, partition) -> partition.createMetrics(builder.registry));

        this.unknownPartition = new Partition("unknown");
        this.unknownPartition.createMetrics(builder.registry);

        this.partitionResolvers = builder.partitionResolvers;
        this.maxDelayedThreads = builder.maxDelayedThreads;

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
    public Optional<Listener> acquire(ContextT context) {
        final Partition partition = resolvePartition(context);

        try {
            lock.lock();
            if (getInflight() >= getLimit() && partition.isLimitExceeded()) {
                lock.unlock();
                if (partition.backoffMillis > 0 && delayedThreads.get() < maxDelayedThreads) {
                    try {
                        delayedThreads.incrementAndGet();
                        TimeUnit.MILLISECONDS.sleep(partition.backoffMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        delayedThreads.decrementAndGet();
                    }
                }

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
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    private void releasePartition(Partition partition) {
        try {
            lock.lock();
            partition.release();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void onNewLimit(int newLimit) {
        super.onNewLimit(newLimit);
        partitions.forEach((name, partition) -> partition.updateLimit(newLimit));
    }

    Partition getPartition(String name) {
        return partitions.get(name);
    }
}
