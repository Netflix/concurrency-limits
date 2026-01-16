package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.SettableLimit;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class DeadlineLimiterTest {
    @Test
    public void test() {
        SettableLimit limit = SettableLimit.startingAt(10);
        DeadlineLimiter<Void> limiter = DeadlineLimiter.wrap(
                SimpleLimiter.newBuilder().limit(limit).build(),
                Instant.now().plusMillis(10));
        
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
        DeadlineLimiter<Void> limiter = DeadlineLimiter.wrap(
                SimpleLimiter.newBuilder().limit(limit).build(),
                Instant.now().plusSeconds(1));
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
    public void testExceedDeadline() {
        Instant deadline = Instant.now().plusMillis(50);
        SettableLimit limit = SettableLimit.startingAt(1);
        DeadlineLimiter<Void> limiter = DeadlineLimiter.wrap(
                SimpleLimiter.newBuilder().limit(limit).build(),
                deadline);

        // Acquire first, will succeed and not block
        limiter.acquire(null);

        // Second acquire should time out after the deadline has been reached
        Assert.assertFalse(limiter.acquire(null).isPresent());
        Instant after = Instant.now();

        assertTrue(after.isAfter(deadline));
    }

    @Test(expected=TimeoutException.class)
    public void testNoTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        SettableLimit limit = SettableLimit.startingAt(1);
        DeadlineLimiter<Void> limiter = DeadlineLimiter.wrap(
                SimpleLimiter.newBuilder().limit(limit).build(),
                Instant.now().plusSeconds(2));
        limiter.acquire(null);

        CompletableFuture<Optional<Limiter.Listener>> future = CompletableFuture.supplyAsync(() -> limiter.acquire(null));
        future.get(1, TimeUnit.SECONDS);
    }
}
