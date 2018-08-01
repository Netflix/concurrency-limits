package com.netflix.concurrency.limits.strategy;

import com.netflix.concurrency.limits.Strategy.Token;
import com.netflix.concurrency.limits.util.Barriers;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleStrategyTest {
    @Test
    public void limitLessThanZeroSetAs1() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        
        strategy.setLimit(-10);
        Assert.assertEquals(1, strategy.getLimit());
    }
    
    @Test
    public void initialState() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Assert.assertEquals(1, strategy.getLimit());
        Assert.assertEquals(0, strategy.getBusyCount());
    }
    
    @Test
    public void acquireIncrementsBusy() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Assert.assertEquals(0, strategy.getBusyCount());
        Assert.assertTrue(strategy.tryAcquire(null).isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void exceedingLimitReturnsFalse() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Assert.assertTrue(strategy.tryAcquire(null).isAcquired());
        Assert.assertFalse(strategy.tryAcquire(null).isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void acquireAndRelease() {
        SimpleStrategy<Void> strategy = new SimpleStrategy<Void>();
        Token completion = strategy.tryAcquire(null);
        Assert.assertTrue(completion.isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
        
        completion.release();
        
        Assert.assertEquals(0, strategy.getBusyCount());

        Assert.assertTrue(strategy.tryAcquire(null).isAcquired());
        Assert.assertEquals(1, strategy.getBusyCount());
    }

    @Test
    public void concurrentAcquire() throws InterruptedException, ExecutionException, TimeoutException {
        int numThreads = Runtime.getRuntime().availableProcessors() - 1;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        SimpleStrategy<Void> strategy = new SimpleStrategy<>();
        int limit = 100;
        strategy.setLimit(limit);
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        try {
            List<Future<?>> futures = IntStream.range(0, numThreads)
                    .mapToObj(x -> executorService.submit(() -> {
                        Barriers.await(barrier);
                        IntStream.range(0, limit).forEach(unused -> strategy.tryAcquire(null));
                    }))
                    .collect(Collectors.toList());
            Barriers.await(barrier);
            for (Future<?> future : futures) {
                future.get(1, TimeUnit.SECONDS);
            }
            Assert.assertEquals(limit, strategy.getBusyCount());
        } finally {
            executorService.shutdown();
        }
    }
}
