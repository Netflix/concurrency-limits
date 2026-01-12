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
package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.internal.Preconditions;
import com.netflix.concurrency.limits.Tags;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public abstract class AbstractLimit implements Limit {
    private volatile int limit;
    private final List<Consumer<Integer>> listeners = new CopyOnWriteArrayList<>();

    protected AbstractLimit(Builder<?> builder) {
        Preconditions.checkArgument(builder.initialLimit >= 0, "initialLimit must be greater than or equal to 0");
        this.limit = builder.initialLimit;
        builder.registry.gauge(MetricIds.LIMIT_NAME, this::getLimit, Tags.ID_NAME, builder.name, Tags.KIND_NAME, builder.kind);
    }

    @Override
    public final synchronized void onSample(long startTime, long rtt, int inflight, boolean didDrop) {
        setLimit(_update(startTime, rtt, inflight, didDrop));
    }

    protected abstract int _update(long startTime, long rtt, int inflight, boolean didDrop);

    @Override
    public final int getLimit() {
        return limit;
    }

    protected synchronized void setLimit(int newLimit) {
        if (newLimit != limit) {
            limit = newLimit;
            listeners.forEach(listener -> listener.accept(newLimit));
        }
    }

    public void notifyOnChange(Consumer<Integer> consumer) {
        this.listeners.add(consumer);
    }

    public abstract static class Builder<BuilderT extends Builder<BuilderT>> {
        private static final AtomicInteger idCounter = new AtomicInteger();

        protected int initialLimit;
        protected String name = "unnamed-" + idCounter.incrementAndGet();
        protected MetricRegistry registry = EmptyMetricRegistry.INSTANCE;
        protected final String kind;

        protected Builder(int initialLimit, String kind) {
            this.initialLimit = initialLimit;
            this.kind = kind;
        }

        /**
         * Initial limit used by the limiter
         * @param initialLimit
         * @return Chainable builder
         */
        public BuilderT initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return self();
        }

        /**
         * Set the name of the limit for metric reporting
         * @param name
         * @return Chainable builder
         */
        public BuilderT named(String name) {
            this.name = name;
            return self();
        }

        /**
         * Registry for reporting metrics about the limit's internal state.
         * @param registry
         * @return Chainable builder
         */
        public BuilderT metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return self();
        }

        protected abstract BuilderT self();

    }

}
