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

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.limit.VegasLimit;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

public abstract class AbstractLimiter<ContextT> implements Limiter<ContextT> {
    public static final String ID_TAG = "id";
    public static final String STATUS_TAG = "status";

    public abstract static class Builder<BuilderT extends Builder<BuilderT>> {
        private static final AtomicInteger idCounter = new AtomicInteger();

        private Limit limit = VegasLimit.newDefault();
        private Supplier<Long> clock = System::nanoTime;

        protected String name = "unnamed-" + idCounter.incrementAndGet();
        protected MetricRegistry registry = EmptyMetricRegistry.INSTANCE;

        private final Predicate<Object> ALWAYS_FALSE = (context) -> false;
        private Predicate<Object> bypassResolver = ALWAYS_FALSE;

        public BuilderT named(String name) {
            this.name = name;
            return self();
        }

        public BuilderT limit(Limit limit) {
            this.limit = limit;
            return self();
        }

        public BuilderT clock(Supplier<Long> clock) {
            this.clock = clock;
            return self();
        }

        public BuilderT metricRegistry(MetricRegistry registry) {
            this.registry = registry;
            return self();
        }

        protected abstract BuilderT self();

        /**
         * Add a chainable bypass resolver predicate from context. Multiple resolvers may be added and if any of the
         * predicate condition returns true the call is bypassed without increasing the limiter inflight count and
         * affecting the algorithm. Will not bypass any calls by default if no resolvers are added.
         *
         * Due to the builders not having access to the ContextT, it is the duty of subclasses to ensure that
         * implementations are type safe.
         *
         * @param shouldBypass Predicate condition to bypass limit
         * @return Chainable builder
         */
        protected BuilderT bypassLimitResolverInternal(Predicate<?> shouldBypass) {
            if (this.bypassResolver == ALWAYS_FALSE) {
                this.bypassResolver = (Predicate<Object>) shouldBypass;
            } else {
                this.bypassResolver = bypassResolver.or((Predicate<Object>) shouldBypass);
            }
            return self();
        }
    }

    private final AtomicInteger inFlight = new AtomicInteger();
    private final Supplier<Long> clock;
    private final Limit limitAlgorithm;
    private final MetricRegistry.Counter successCounter;
    private final MetricRegistry.Counter droppedCounter;
    private final MetricRegistry.Counter ignoredCounter;
    private final MetricRegistry.Counter rejectedCounter;
    private final MetricRegistry.Counter bypassCounter;
    private Predicate<ContextT> bypassResolver = (context) -> false;

    private volatile int limit;

    protected AbstractLimiter(Builder<?> builder) {
        this.clock = builder.clock;
        this.limitAlgorithm = builder.limit;
        this.limit = limitAlgorithm.getLimit();
        this.limitAlgorithm.notifyOnChange(this::onNewLimit);
        this.bypassResolver = (Predicate<ContextT>) builder.bypassResolver;

        builder.registry.gauge(MetricIds.LIMIT_NAME, this::getLimit);
        this.successCounter = builder.registry.counter(MetricIds.CALL_NAME, ID_TAG, builder.name, STATUS_TAG, "success");
        this.droppedCounter = builder.registry.counter(MetricIds.CALL_NAME, ID_TAG, builder.name, STATUS_TAG, "dropped");
        this.ignoredCounter = builder.registry.counter(MetricIds.CALL_NAME, ID_TAG, builder.name, STATUS_TAG, "ignored");
        this.rejectedCounter = builder.registry.counter(MetricIds.CALL_NAME, ID_TAG, builder.name, STATUS_TAG, "rejected");
        this.bypassCounter = builder.registry.counter(MetricIds.CALL_NAME, ID_TAG, builder.name, STATUS_TAG, "bypassed");
    }

    protected boolean shouldBypass(ContextT context){
        return bypassResolver.test(context);
    }

    protected Optional<Listener> createRejectedListener() {
        this.rejectedCounter.increment();
        return Optional.empty();
    }

    protected Optional<Listener> createBypassListener() {
        this.bypassCounter.increment();
        return Optional.of(new Listener() {

            @Override
            public void onSuccess() {
                // Do nothing
            }

            @Override
            public void onIgnore() {
                // Do nothing
            }

            @Override
            public void onDropped() {
                // Do nothing
            }
        });
    }

    protected Listener createListener() {
        final long startTime = clock.get();
        final int currentInflight = inFlight.incrementAndGet();
        return new Listener() {
            @Override
            public void onSuccess() {
                inFlight.decrementAndGet();
                successCounter.increment();

                limitAlgorithm.onSample(startTime, clock.get() - startTime, currentInflight, false);
            }

            @Override
            public void onIgnore() {
                inFlight.decrementAndGet();
                ignoredCounter.increment();
            }

            @Override
            public void onDropped() {
                inFlight.decrementAndGet();
                droppedCounter.increment();

                limitAlgorithm.onSample(startTime, clock.get() - startTime, currentInflight, true);
            }
        };
    }

    public int getLimit() {
        return limit;
    }

    public int getInflight() { return inFlight.get(); }

    protected void onNewLimit(int newLimit) {
        limit = newLimit;
    }

}
