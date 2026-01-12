/**
 * Copyright 2025 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.bulkhead;

import com.netflix.concurrency.limits.Bulkhead;
import com.netflix.concurrency.limits.Bulkhead.LowCardinalityContextBulkhead;
import com.netflix.concurrency.limits.Bulkhead.ParallelDrainingBulkhead;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * A non-blocking {@link Bulkhead} that maintains a partial FIFO ordering of task execution; tasks
 * with equal contexts are executed in FIFO order, but tasks with different contexts may be
 * reordered to allow parallel draining. This class is intended for use with low-cardinality enum
 * contexts, where each context gets its own {@link Bulkhead} (and backlog) to allow parallel
 * draining.
 *
 * @param <ContextT> the {@link Enum} context type to run tasks with
 * @see FifoParallelDispatcherBulkhead
 */
public class EnumContextPartialFifoOrderBulkhead<ContextT extends Enum<ContextT>> implements
        LowCardinalityContextBulkhead<ContextT>,
        ParallelDrainingBulkhead<ContextT> {

    private final Map<ContextT, Bulkhead<ContextT>> bulkheads;

    private EnumContextPartialFifoOrderBulkhead(Map<ContextT, Bulkhead<ContextT>> bulkheads) {
        this.bulkheads = bulkheads;
    }

    @Override
    public <T> CompletionStage<T> executeCompletionStage(Supplier<? extends CompletionStage<T>> supplier,
                                                         ContextT context) {
        return bulkheads.get(context).executeCompletionStage(supplier, context);
    }

    public static <ContextT extends Enum<ContextT>> Builder<ContextT> newBuilder(Class<ContextT> clazz) {
        return new Builder<>(clazz);
    }

    public static class Builder<ContextT extends Enum<ContextT>> extends
            AbstractDispatcherBulkhead.AbstractBuilder<ContextT, Builder<ContextT>> {

        private final Class<ContextT> clazz;

        private Builder(Class<ContextT> clazz) {
            this.clazz = clazz;
        }

        @Override
        public EnumContextPartialFifoOrderBulkhead<ContextT> build() {
            final Map<ContextT, Bulkhead<ContextT>> bulkheads = new EnumMap<>(clazz);
            for (ContextT context : clazz.getEnumConstants()) {
                final Bulkhead<ContextT> bulkhead = new SingletonContextDispatcherBulkhead<>(
                        limiter, backlog, exceptionClassifier, maxDispatchPerCall, context);
                bulkheads.put(context, bulkhead);
            }
            return new EnumContextPartialFifoOrderBulkhead<>(bulkheads);
        }
    }
}
