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
import com.netflix.concurrency.limits.Bulkhead.ParallelDrainingBulkhead;
import com.netflix.concurrency.limits.Limiter;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;

/**
 * A non-blocking dispatcher {@link Bulkhead} with round-robin style task execution; when no permit
 * can be obtained from the {@link Limiter}, tasks are added back to (the tail) of the queue. This
 * means there is no FIFO guarantee on the order of task execution. However, draining the queue can
 * be done in parallel, which may provide higher throughput, but higher latencies than
 * {@link FifoSerialDispatcherBulkhead} in some scenarios.
 *
 * @param <ContextT> the context type to run tasks with
 * @see AbstractDispatcherBulkhead
 */
public class RoundRobinDispatcherBulkhead<ContextT> extends AbstractDispatcherBulkhead<ContextT>
        implements ParallelDrainingBulkhead<ContextT> {

    private RoundRobinDispatcherBulkhead(Limiter<ContextT> limiter,
                                         Queue<BulkheadTask<?, ContextT>> backlog,
                                         Function<Throwable, Limiter.Listener.Result> exceptionClassifier,
                                         int maxDispatchPerCall) {
        super(limiter, backlog, exceptionClassifier, maxDispatchPerCall);
    }

    @Override
    protected void drain() {
        BulkheadTask<?, ContextT> head;
        int dispatched;
        for (head = backlog.poll(), dispatched = 0;
             head != null && dispatched < maxDispatchPerCall;
             head = backlog.poll(), dispatched++) {
            final Optional<Limiter.Listener> listener = limiter.acquire(head.context);
            if (!listener.isPresent()) {
                try {
                    backlog.add(head);
                } catch (IllegalStateException ise) {
                    head.result.completeExceptionally(new RejectedExecutionException("Backlog full", ise));
                }
                return;
            }

            dispatch(head, listener.get());
        }
    }

    public static <ContextT> Builder<ContextT> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<ContextT> extends AbstractBuilder<ContextT, Builder<ContextT>> {

        public RoundRobinDispatcherBulkhead<ContextT> build() {
            return new RoundRobinDispatcherBulkhead<>(limiter, backlog, exceptionClassifier, maxDispatchPerCall);
        }
    }
}
