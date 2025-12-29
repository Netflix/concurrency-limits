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

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class FifoParallelDispatcherBulkheadTest {

    @Test
    public void testSuccessfulExecution() {
        Limiter<Void> limiter = SimpleLimiter.newBuilder().limit(FixedLimit.of(1)).build();

        FifoParallelDispatcherBulkhead bulkhead = FifoParallelDispatcherBulkhead.newBuilder()
                .limiter(limiter)
                .backlog(1)
                .exceptionClassifier(t -> Limiter.Listener.Result.IGNORE)
                .maxDispatchPerCall(1)
                .build();

        AtomicBoolean executed = new AtomicBoolean(false);

        CompletionStage<?> result = bulkhead.executeCompletionStage(
                () -> CompletableFuture.runAsync(
                        () -> executed.set(true)), null);

        result.toCompletableFuture().join();

        Assert.assertTrue(executed.get());
    }
}
