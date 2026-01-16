package com.netflix.concurrency.limits.executor;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor;
import com.netflix.concurrency.limits.limit.SettableLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingAdaptiveExecutorTest {

    @Test
    public void testBuilderWithNoLimiter() {
        BlockingAdaptiveExecutor executor = BlockingAdaptiveExecutor.newBuilder().build();
        AtomicBoolean executed = new AtomicBoolean(false);
        executor.execute(() -> executed.set(true));

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Assert.assertTrue("Task should have been executed", executed.get());
    }

    @Test
    public void testBuilderWithSimpleLimiter() throws InterruptedException {
        SettableLimit limit = SettableLimit.startingAt(2);
        Limiter<Void> simpleLimiter = SimpleLimiter.newBuilder()
            .limit(limit)
            .build();

        BlockingAdaptiveExecutor executor = BlockingAdaptiveExecutor.newBuilder()
            .limiter(simpleLimiter)
            .build();

        CountDownLatch taskStarted = new CountDownLatch(2);
        CountDownLatch taskComplete = new CountDownLatch(1);
        CountDownLatch thirdTaskComplete = new CountDownLatch(1);

        for (int i = 0; i < 2; i++) {
            executor.execute(() -> {
                taskStarted.countDown();
                try {
                    taskComplete.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        Assert.assertTrue("Tasks should start", taskStarted.await(1, TimeUnit.SECONDS));

        AtomicBoolean thirdTaskStarted = new AtomicBoolean(false);
        Thread blockedThread = new Thread(() ->
            executor.execute(() -> {
                thirdTaskStarted.set(true);
                thirdTaskComplete.countDown();
        }));

        blockedThread.start();
        Thread.sleep(200);

        Assert.assertFalse("Third task should be blocked", thirdTaskStarted.get());

        taskComplete.countDown();

        Assert.assertTrue("Third task should eventually execute",
                thirdTaskComplete.await(1, TimeUnit.SECONDS));
    }

}
