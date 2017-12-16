package com.netflix.concurrency.limits.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.SettableLimit;
import com.netflix.concurrency.limits.strategy.SimpleStrategy;

import java.util.LinkedList;

import org.junit.Test;

public class BlockingLimiterTest {
    @Test
    public void test() {
        SettableLimit limit = SettableLimit.startingAt(10);
        BlockingLimiter<Void> limiter = BlockingLimiter.wrap(new DefaultLimiter<>(limit, new SimpleStrategy()));
        
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
}
