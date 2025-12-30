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
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;

public class EnumContextPartialFifoOrderBulkheadTest {

    @Test
    public void testSuccessfulExecution() {
        Limiter<TestContext> limiter = new TestLimiterBuilder()
                .partition(TestContext.REQUEST.name(), .5)
                .partition(TestContext.RESPONSE.name(), .5)
                .partitionResolver(TestContext::name)
                .limit(FixedLimit.of(2))
                .build();

        EnumContextPartialFifoOrderBulkhead<TestContext> bulkhead = EnumContextPartialFifoOrderBulkhead.newBuilder(TestContext.class)
                .limiter(limiter)
                .backlog(1)
                .exceptionClassifier(t -> Limiter.Listener.Result.IGNORE)
                .maxDispatchPerCall(1)
                .build();

        AtomicBoolean requestExecuted = new AtomicBoolean(false);

        CompletionStage<?> requestResult = bulkhead.executeCompletionStage(
                () -> CompletableFuture.runAsync(
                        () -> requestExecuted.set(true)), TestContext.REQUEST);

        AtomicBoolean responseExecuted = new AtomicBoolean(false);

        CompletionStage<?> responseResult = bulkhead.executeCompletionStage(
                () -> CompletableFuture.runAsync(
                        () -> responseExecuted.set(true)), TestContext.RESPONSE);

        requestResult.toCompletableFuture().join();
        responseResult.toCompletableFuture().join();

        Assert.assertTrue(requestExecuted.get());
        Assert.assertTrue(responseExecuted.get());
    }

    static class TestLimiterBuilder extends AbstractPartitionedLimiter.Builder<TestLimiterBuilder, TestContext> {

        @Override
        protected TestLimiterBuilder self() {
            return this;
        }
    }

    enum TestContext {
        REQUEST,
        RESPONSE
    }
}
