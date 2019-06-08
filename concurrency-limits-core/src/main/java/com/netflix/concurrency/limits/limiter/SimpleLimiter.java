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

import com.netflix.concurrency.limits.MetricIds;
import com.netflix.concurrency.limits.MetricRegistry;

import java.util.Optional;

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

    public SimpleLimiter(AbstractLimiter.Builder<?> builder) {
        super(builder);

        this.inflightDistribution = builder.registry.distribution(MetricIds.INFLIGHT_NAME);
    }

    @Override
    public Optional<Listener> acquire(ContextT context) {
        int currentInFlight = getInflight();
        inflightDistribution.addSample(currentInFlight);
        if (currentInFlight >= getLimit()) {
            return createRejectedListener();
        }
        return Optional.of(createListener());
    }
}
