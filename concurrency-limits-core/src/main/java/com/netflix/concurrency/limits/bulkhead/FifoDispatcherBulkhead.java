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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * A non-blocking dispatcher {@link Bulkhead} that guarantees FIFO ordering of task execution.
 * <p>
 * The downside of maintaining this strict FIFO ordering is that draining cannot be done in
 * parallel, hence {@link RoundRobinDispatcherBulkhead} may provide higher throughput in some
 * scenarios. However, FIFO ordering may provide lower latencies.
 *
 * @param <ContextT> the context type to run tasks with
 * @see AbstractDispatcherBulkhead
 */
public class FifoDispatcherBulkhead<ContextT> extends AbstractDispatcherBulkhead<ContextT> {

    private final AtomicInteger wip = new AtomicInteger();

    private FifoDispatcherBulkhead(Limiter<ContextT> limiter,
                                   BlockingQueue<BulkheadTask<?, ContextT>> backlog,
                                   Function<Throwable, Limiter.Listener.Result> exceptionClassifier,
                                   int maxDispatchPerCall) {
        super(limiter, backlog, exceptionClassifier, maxDispatchPerCall);
    }

    public int getWip() {
        return wip.get();
    }

    @Override
    protected void drain() {
        if (wip.getAndIncrement() == 0) {
            drainLoop();
        }
    }

    private void drainLoop() {
        int todo = 1;

        while (todo > 0) {
            int dispatched = 0;
            BulkheadTask<?, ContextT> head;
            while (dispatched < maxDispatchPerCall && (head = backlog.peek()) != null) {
                final Optional<Limiter.Listener> listener = limiter.acquire(head.context);
                if (!listener.isPresent()) {
                    break;
                }

                head = backlog.poll();
                if (head == null) {
                    listener.get().onIgnore();
                } else {
                    dispatch(head, listener.get());
                    dispatched++;
                }
            }

            todo = wip.addAndGet(-todo);
        }
    }

    public static <ContextT> Builder<ContextT> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<ContextT> extends AbstractBuilder<ContextT, Builder<ContextT>> {

        @Override
        public FifoDispatcherBulkhead<ContextT> build() {
            return new FifoDispatcherBulkhead<>(limiter, backlog, exceptionClassifier, maxDispatchPerCall);
        }
    }
}
