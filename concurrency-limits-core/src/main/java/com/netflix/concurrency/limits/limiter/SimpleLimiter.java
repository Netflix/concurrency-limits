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

import java.util.Optional;
import java.util.concurrent.Semaphore;

public class SimpleLimiter<ContextT> extends AbstractLimiter<ContextT> {
    public static class Builder extends AbstractLimiter.Builder<Builder> {
        public <ContextT> SimpleLimiter<ContextT> build() {
            return new SimpleLimiter<>(this);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }
    private final MetricRegistry.SampleListener inflightDistribution;
    private final AdjustableSemaphore semaphore;

    public SimpleLimiter(AbstractLimiter.Builder<?> builder) {
        super(builder);

        this.inflightDistribution = builder.registry.distribution(MetricIds.INFLIGHT_NAME);
        this.semaphore = new AdjustableSemaphore(getLimit());
    }

    @Override
    public Optional<Limiter.Listener> acquire(ContextT context) {
        Optional<Limiter.Listener> listener;
        if (shouldBypass(context)) {
            listener = createBypassListener();
        }
        else if (!semaphore.tryAcquire()) {
            listener = createRejectedListener();
        }
        else {
            listener = Optional.of(new Listener(createListener()));
        }
        inflightDistribution.addLongSample(getInflight());
        return listener;
    }

    @Override
    protected void onNewLimit(int newLimit) {
        int oldLimit = this.getLimit();
        super.onNewLimit(newLimit);

        if (newLimit > oldLimit) {
            semaphore.release(newLimit - oldLimit);
        } else {
            semaphore.reducePermits(oldLimit - newLimit);
        }
    }

    /**
     * Simple Semaphore subclass that allows access to its reducePermits method.
     */
    private static final class AdjustableSemaphore extends Semaphore {
        AdjustableSemaphore(int permits) {
            super(permits);
        }

        @Override
        public void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }

    private class Listener implements Limiter.Listener {
        private final Limiter.Listener delegate;

        Listener(Limiter.Listener delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSuccess() {
            delegate.onSuccess();
            semaphore.release();
        }

        @Override
        public void onIgnore() {
            delegate.onIgnore();
            semaphore.release();
        }

        @Override
        public void onDropped() {
            delegate.onDropped();
            semaphore.release();
        }
    }
}
