package com.netflix.concurrency.limits.executor;
import com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor;
 
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.SettableLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import org.junit.Assert;
import org.junit.Test;
 
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
 
public class BlockingAdaptiveExecutorMaxThreadsTest {
 
    @Test
    public void defaultExecutorRejectsInsteadOfCreatingUnboundedThreads() throws InterruptedException {
        // The limiter deliberately allows far more concurrency than the thread cap,
        // simulating a misconfigured or runaway limit.
        Limiter<Void> limiter = SimpleLimiter.newBuilder()
                .limit(SettableLimit.startingAt(100))
                .build();
 
        BlockingAdaptiveExecutor executor = BlockingAdaptiveExecutor.newBuilder()
                .limiter(limiter)
                .maxThreads(2)
                .build();
 
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
 
        try {
            for (int i = 0; i < 2; i++) {
                executor.execute(() -> {
                    started.countDown();
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            Assert.assertTrue("Both tasks should be running", started.await(1, TimeUnit.SECONDS));
 
            try {
                executor.execute(() -> { });
                Assert.fail("Expected RejectedExecutionException once maxThreads threads are busy");
            } catch (RejectedExecutionException expected) {
                // Expected: the pool refuses the task instead of spawning a third thread.
            }
        } finally {
            release.countDown();
        }
    }
 
    @Test(expected = IllegalArgumentException.class)
    public void maxThreadsMustBePositive() {
        BlockingAdaptiveExecutor.newBuilder().maxThreads(0);
    }
}