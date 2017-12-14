package com.netflix.concurrency.limits.strategy;

import com.netflix.concurrency.limits.Strategy;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simplest strategy for enforcing a concurrency limit that has a single counter
 * for tracking total usage.
 */
public final class SimpleStrategy implements Strategy<Void> {

    private AtomicInteger busy = new AtomicInteger();
    private volatile int limit = 1;
    
    @Override
    public boolean tryAcquire(Void context) {
        if (busy.get() >= limit) {
            return false;
        }
        
        busy.incrementAndGet();
        return true;
    }
    
    @Override
    public void release(Void context) {
        busy.decrementAndGet();
    }

    @Override
    public void setLimit(int limit) {
        if (limit < 1) {
            limit = 1;
        }
        this.limit = limit;
    }
    
    // Visible for testing
    int getLimit() {
        return limit;
    }
    
    int getBusyCount() {
        return busy.get();
    }

    @Override
    public String toString() {
        return "SimpleStrategy [busy=" + busy.get() + ", limit=" + limit + "]";
    }
}
