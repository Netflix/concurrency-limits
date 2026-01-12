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
import com.netflix.concurrency.limits.Bulkhead.GlobalParallelDrainingBulkhead;
import com.netflix.concurrency.limits.Limiter;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;

/**
 * A non-blocking dispatcher {@link Bulkhead} that guarantees FIFO ordering of task execution. Does
 * not support contexts for task execution in favor of parallelism.
 *
 * @see AbstractDispatcherBulkhead
 */
public class FifoParallelDispatcherBulkhead
        extends AbstractDispatcherBulkhead<Void>
        implements GlobalParallelDrainingBulkhead {

    private FifoParallelDispatcherBulkhead(Limiter<Void> limiter,
                                           Queue<BulkheadTask<?, Void>> backlog,
                                           Function<Throwable, Limiter.Listener.Result> exceptionClassifier,
                                           int maxDispatchPerCall) {
        super(limiter, backlog, exceptionClassifier, maxDispatchPerCall);
    }


    @Override
    protected void drain() {
        int dispatched = 0;
        while (dispatched < maxDispatchPerCall && !backlog.isEmpty()) {
            final Optional<Limiter.Listener> listener = limiter.acquire(null);
            if (!listener.isPresent()) {
                return;
            }

            final BulkheadTask<?, Void> head = backlog.poll();
            if (head == null) {
                listener.get().onIgnore();
            } else {
                dispatch(head, listener.get());
                dispatched++;
            }
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<Void, Builder> {

        @Override
        public FifoParallelDispatcherBulkhead build() {
            return new FifoParallelDispatcherBulkhead(limiter, backlog, exceptionClassifier, maxDispatchPerCall);
        }
    }
}
