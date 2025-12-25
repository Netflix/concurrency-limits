/**
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.concurrency.limits;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;

/**
 * A bulkhead that can limit concurrent executions with a given context.
 *
 * @param <ContextT> the context type to run tasks with
 */
public interface Bulkhead<ContextT> {

    /**
     * Executes the given {@link CompletionStage} with the given context.
     *
     * @param supplier the task to run
     * @param context  the context to run the task with
     * @param <T>      the type of the {@link CompletionStage}s
     * @return a new {@link CompletionStage} with the same completion result as the supplier,
     * except for when this bulkhead cannot accept the task, in which case it completes
     * exceptionally with a {@link RejectedExecutionException}
     */
    <T> CompletionStage<T> executeCompletionStage(Supplier<? extends CompletionStage<T>> supplier, ContextT context);

    /**
     * A bulkhead that drains the backlog in parallel and executes its tasks in parallel (while
     * limiting concurrency). This bulkhead offers additional methods to run synchronous tasks in
     * calling threads (which is not recommended in a serial drainer).
     *
     * @param <ContextT> the context type to run tasks with
     */
    interface ParallelDrainingBulkhead<ContextT> extends Bulkhead<ContextT> {

        /**
         * Executes the given {@link Supplier} with the given context.
         *
         * @param supplier the task to run
         * @param context  the context to run the task with
         * @param <T>      the type of the result
         * @return a {@link CompletionStage} with a completion result set by running the supplier,
         * except for when this bulkhead cannot accept the task, in which case it completes
         * exceptionally with a {@link RejectedExecutionException}
         */
        default <T> CompletionStage<T> executeSupplier(Supplier<T> supplier, ContextT context) {
            return executeCompletionStage(() -> {
                try {
                    return CompletableFuture.completedFuture(supplier.get());
                } catch (Throwable t) {
                    CompletableFuture<T> failed = new CompletableFuture<>();
                    failed.completeExceptionally(t);
                    return failed;
                }
            }, context);
        }

        /**
         * Executes the given {@link Runnable} with the given context.
         *
         * @param runnable the task to run
         * @param context  the context to run the task with
         * @return a {@link CompletionStage} with a completion result set by running the runnable,
         * except for when this bulkhead cannot accept the task, in which case it completes
         * exceptionally with a {@link RejectedExecutionException}
         */
        default CompletionStage<Void> execute(Runnable runnable, ContextT context) {
            return executeSupplier(() -> {
                runnable.run();
                return null;
            }, context);
        }

        /**
         * Executes the given {@link Callable} with the given context.
         *
         * @param callable the task to run
         * @param context  the context to run the task with
         * @return a {@link CompletionStage} with a completion result set by running the callable,
         * except for when this bulkhead cannot accept the task, in which case it completes
         * exceptionally with a {@link RejectedExecutionException}
         */
        default <T> CompletionStage<T> execute(Callable<T> callable, ContextT context) {
            return executeSupplier(() -> {
                try {
                    return callable.call();
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, context);
        }
    }
}
