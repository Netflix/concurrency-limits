package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.MetricRegistry;
import com.netflix.concurrency.limits.internal.EmptyMetricRegistry;
import com.netflix.concurrency.limits.limit.VegasLimit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public abstract class AbstractLimiter<ContextT> implements Limiter<ContextT> {
    public abstract static class Builder<BuilderT extends Builder<BuilderT, ContextT>, ContextT> {
        private Limit limit = VegasLimit.newDefault();
        private Supplier<Long> clock = System::nanoTime;
        protected MetricRegistry registry = EmptyMetricRegistry.INSTANCE;

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
    }

    private final AtomicInteger inFlight = new AtomicInteger();
    private final Supplier<Long> clock;
    private final Limit limitAlgorithm;
    private volatile int limit;

    protected AbstractLimiter(Builder<?, ContextT> builder) {
        this.clock = builder.clock;
        this.limitAlgorithm = builder.limit;
        this.limit = limitAlgorithm.getLimit();
        this.limitAlgorithm.notifyOnChange(this::onNewLimit);
    }

    protected Listener createListener() {
        final long startTime = clock.get();
        final int currentInflight = inFlight.incrementAndGet();
        return new Listener() {
            @Override
            public void onSuccess() {
                inFlight.decrementAndGet();

                limitAlgorithm.onSample(startTime, clock.get() - startTime, currentInflight, false);
            }

            @Override
            public void onIgnore() {
                inFlight.decrementAndGet();
            }

            @Override
            public void onDropped() {
                inFlight.decrementAndGet();

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
