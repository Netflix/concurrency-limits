package com.netflix.concurrency.limits.executor;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Ignore;
import org.junit.Test;

import com.netflix.concurrency.limits.executors.BlockingAdaptiveExecutor;
import com.netflix.concurrency.limits.limit.AIMDLimit;
import com.netflix.concurrency.limits.limit.GradientLimit;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.DefaultLimiter;
import com.netflix.concurrency.limits.strategy.SimpleStrategy;

@Ignore("These are simulations and not tests")
public class BlockingAdaptiveExecutorSimulation {
    @Test
    public void test() {
        DefaultLimiter<Void> limiter = DefaultLimiter.newBuilder().limit(AIMDLimit.newBuilder().initialLimit(10).build()).build(new SimpleStrategy<>());
        Executor executor = new BlockingAdaptiveExecutor(limiter);
        
        run(10000, 20, executor, randomLatency(50, 150));
    }
    
    @Test
    public void testVegas() {
        DefaultLimiter<Void> limiter = DefaultLimiter.newBuilder().limit(VegasLimit.newBuilder().initialLimit(100).build()).build(new SimpleStrategy<>());
        Executor executor = new BlockingAdaptiveExecutor(limiter);
        run(10000, 50, executor, randomLatency(50, 150));
    }
    
    @Test
    public void testGradient() {
        DefaultLimiter<Void> limiter = DefaultLimiter.newBuilder().limit(GradientLimit.newBuilder().initialLimit(100).build()).build(new SimpleStrategy<>());
        Executor executor = new BlockingAdaptiveExecutor(limiter);
        run(100000, 50, executor, randomLatency(50, 150));
    }
    
    public void run(int iterations, int limit, Executor executor, Supplier<Long> latency) {
        AtomicInteger requests = new AtomicInteger();
        AtomicInteger busy = new AtomicInteger();
        
        AtomicInteger counter = new AtomicInteger();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println("" + counter.incrementAndGet() + " total=" + requests.getAndSet(0) + " busy=" + busy.get());
        }, 1, 1, TimeUnit.SECONDS);

        Semaphore sem = new Semaphore(limit, true);
        for (int i = 0; i < iterations; i++) {
            requests.incrementAndGet();
            busy.incrementAndGet();
            executor.execute(() -> {
                try {
                    sem.acquire();
                    TimeUnit.MILLISECONDS.sleep(latency.get()); 
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    sem.release();
                    busy.decrementAndGet();
                }
            });
        }
    }
    
    public Supplier<Long> randomLatency(int min, int max) {
        return () -> min + ThreadLocalRandom.current().nextLong(max - min);
    }
    
    public void sleepNoThrow(long timeout, TimeUnit units) {
        try {
            units.sleep(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
