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
}
