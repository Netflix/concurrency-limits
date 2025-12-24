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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A non-blocking FIFO dispatcher {@link Bulkhead} that uses a {@link Limiter} to control
 * concurrency and a backlog {@link BlockingQueue} to hold pending tasks.
 * <p>
 * Using this {@link Bulkhead} is suitable in cases where dispatching tasks is cheap and can be done
 * by threads calling {@link #executeCompletionStage(Supplier, Object)}, or threads that complete
 * the dispatched tasks. Ideally, the actual work of these tasks, e.g., the transport of gRPC calls,
 * is done by a separate {@link Executor}. This bulkhead however guarantees there are no more
 * concurrent tasks running beyond what the given {@link Limiter} allows.
 *
 * @param <ContextT> the context type to run tasks with
 */
public class FifoDispatcherBulkhead<ContextT> implements Bulkhead<ContextT> {

    private final Limiter<ContextT> limiter;

    private final BlockingQueue<BulkheadTask<?, ContextT>> backlog;

    private final Function<Throwable, Limiter.Listener.Result> exceptionClassifier;

    private final AtomicInteger wip = new AtomicInteger();

    private FifoDispatcherBulkhead(Limiter<ContextT> limiter,
                                   BlockingQueue<BulkheadTask<?, ContextT>> backlog,
                                   Function<Throwable, Limiter.Listener.Result> exceptionClassifier) {
        this.limiter = limiter;
        this.backlog = backlog;
        this.exceptionClassifier = exceptionClassifier;
    }

    public int getWip() {
        return wip.get();
    }

    public int getBacklogSize() {
        return backlog.size();
    }

    @Override
    public <T> CompletionStage<T> executeCompletionStage(Supplier<? extends CompletionStage<T>> supplier, ContextT context) {
        final CompletableFuture<T> result = new CompletableFuture<>();

        try {
            backlog.add(new BulkheadTask<>(supplier, result, context));
            signalDrain();
        } catch (IllegalStateException ise) {
            result.completeExceptionally(new RejectedExecutionException("Backlog full", ise));
        }

        return result;
    }

    private void signalDrain() {
        if (wip.getAndIncrement() == 0) {
            drainLoop();
        }
    }

    private void drainLoop() {
        int todo = 1;

        while (todo > 0) {
            BulkheadTask<?, ContextT> head;
            while ((head = backlog.peek()) != null) {
                final Optional<Limiter.Listener> listener = limiter.acquire(head.context);
                if (!listener.isPresent()) {
                    break;
                }

                head = backlog.poll();
                if (head == null) {
                    listener.get().onIgnore();
                } else {
                    dispatch(head, listener.get());
                }
            }

            todo = wip.addAndGet(-todo);
        }
    }


    private <T> void dispatch(BulkheadTask<T, ContextT> task, Limiter.Listener listener) {
        final CompletionStage<T> stage;
        try {
            stage = task.supplier.get();
        } catch (RuntimeException re) {
            // Failed before a meaningful RTT measurement could be made.
            listener.onIgnore();
            task.result.completeExceptionally(re);
            signalDrain();
            return;
        }

        stage.whenComplete(
                (value, throwable) -> {
                    try {
                        if (throwable == null) {
                            listener.onSuccess();
                            task.result.complete(value);
                        } else {
                            Limiter.Listener.Result result = classifyException(throwable);
                            listener.on(result);
                            task.result.completeExceptionally(throwable);
                        }
                    } finally {
                        // Completion frees capacity; kick the drainer to fill newly available tokens.
                        signalDrain();
                    }
                });
    }

    private Limiter.Listener.Result classifyException(Throwable throwable) {
        return (throwable instanceof CompletionException || throwable instanceof ExecutionException)
                && throwable.getCause() != null
                ? classifyException(throwable.getCause())
                : exceptionClassifier.apply(throwable);

    }

    public static <ContextT> Builder<ContextT> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<ContextT> {

        private Limiter<ContextT> limiter;

        private BlockingQueue<BulkheadTask<?, ContextT>> backlog;

        private Function<Throwable, Limiter.Listener.Result> exceptionClassifier;

        public Builder<ContextT> limiter(Limiter<ContextT> limiter) {
            this.limiter = limiter;
            return this;
        }

        private Builder<ContextT> backlog(BlockingQueue<BulkheadTask<?, ContextT>> backlog) {
            this.backlog = backlog;
            return this;
        }

        public Builder<ContextT> backlog(int size) {
            if (size < 0) {
                return backlog(new LinkedBlockingQueue<>());
            } else if (size == 0) {
                return backlog(new SynchronousQueue<>());
            } else if (size >= 10_000) {
                return backlog(new LinkedBlockingQueue<>(size));
            } else {
                return backlog(new ArrayBlockingQueue<>(size));
            }
        }

        public Builder<ContextT> exceptionClassifier(Function<Throwable, Limiter.Listener.Result> exceptionClassifier) {
            this.exceptionClassifier = exceptionClassifier;
            return this;
        }

        public FifoDispatcherBulkhead<ContextT> build() {
            return new FifoDispatcherBulkhead<>(limiter, backlog, exceptionClassifier);
        }
    }

    private static class BulkheadTask<T, ContextT> {

        final Supplier<? extends CompletionStage<T>> supplier;

        final CompletableFuture<T> result;

        final ContextT context;

        BulkheadTask(Supplier<? extends CompletionStage<T>> supplier, CompletableFuture<T> result, ContextT context) {
            this.supplier = supplier;
            this.result = result;
            this.context = context;
        }
    }
}
