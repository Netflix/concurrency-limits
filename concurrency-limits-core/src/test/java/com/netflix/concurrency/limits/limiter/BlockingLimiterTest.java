package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.SettableLimit;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BlockingLimiterTest {
    @Test
    public void test() {
        SettableLimit limit = SettableLimit.startingAt(10);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build());
        
        LinkedList<Limiter.Listener> listeners = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            limiter.acquire(null).ifPresent(listeners::add);
        }
        
        limit.setLimit(1);
        
        while (!listeners.isEmpty()) {
            listeners.remove().onSuccess();
        }
        
        limiter.acquire(null);
    }

    @Test
    public void testMultipleBlockedThreads() throws InterruptedException, ExecutionException, TimeoutException {
        int numThreads = 8;
        SettableLimit limit = SettableLimit.startingAt(1);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build());
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            for (Future<?> future : IntStream.range(0, numThreads)
                    .mapToObj(x -> executorService.submit(() -> limiter.acquire(null).get().onSuccess()))
                    .collect(Collectors.toList())) {
                future.get(1, TimeUnit.SECONDS);
            }
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testTimeout() {
        Duration timeout = Duration.ofMillis(50);
        SettableLimit limit = SettableLimit.startingAt(1);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(limit).build(), timeout);
        limiter.acquire(null);
        Instant before = Instant.now();
        assertEquals(Optional.empty(), limiter.acquire(null));
        Instant after = Instant.now();
        Duration interval = Duration.between(before, after);
        assertTrue(interval.compareTo(timeout) >= 0);
    }
}
