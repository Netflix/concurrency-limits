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
import com.netflix.concurrency.limits.Limiter;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;

/**
 * A non-blocking dispatcher {@link Bulkhead} that guarantees FIFO ordering of task execution for a
 * given fixed context. Since there is only one context, draining can be done in parallel while
 * maintaining FIFO (because permits for tasks with equal contexts can be exchanged freely). This
 * class is for internal use only; clients should use {@link EnumContextPartialFifoOrderBulkhead} or
 * {@link FifoParallelDispatcherBulkhead}.
 *
 * @param <ContextT> the context type to run tasks with
 * @see EnumContextPartialFifoOrderBulkhead
 * @see FifoParallelDispatcherBulkhead
 */
class SingletonContextDispatcherBulkhead<ContextT> extends AbstractDispatcherBulkhead<ContextT> {

    private final ContextT context;

    SingletonContextDispatcherBulkhead(Limiter<ContextT> limiter,
                                       Queue<BulkheadTask<?, ContextT>> backlog,
                                       Function<Throwable, Limiter.Listener.Result> exceptionClassifier,
                                       int maxDispatchPerCall,
                                       ContextT context) {
        super(limiter, backlog, exceptionClassifier, maxDispatchPerCall);
        this.context = context;
    }

    @Override
    protected void drain() {
        int dispatched = 0;
        BulkheadTask<?, ContextT> head;
        while (dispatched < maxDispatchPerCall && (head = backlog.peek()) != null) {
            assert head.context == context;
            final Optional<Limiter.Listener> listener = limiter.acquire(context);
            if (!listener.isPresent()) {
                return;
            }

            head = backlog.poll();
            if (head == null) {
                listener.get().onIgnore();
            } else {
                assert head.context == context;
                dispatch(head, listener.get());
                dispatched++;
            }
        }

    }
}
