package com.netflix.concurrency.limits.strategy;

import com.netflix.concurrency.limits.Strategy;
import com.netflix.concurrency.limits.internal.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Concurrency limiter that guarantees a certain percentage of the limit to specific callers
 * while allowing callers to borrow from underutilized callers.
 * 
 * Callers are identified by their index into an array of percentages passed in during initialization.
 * A percentage of 0.0 means that a caller may only use excess capacity and can be completely 
 * starved when the limit is reached.  A percentage of 1.0 means that the caller
 * is guaranteed to get the entire limit.
 */
public final class PercentageStrategy implements Strategy<Integer> {

    private final List<Bin> bins;
    private int busy = 0;
    private int limit = 0;
    
    public PercentageStrategy(List<Double> bins) {
        Preconditions.checkArgument(bins.stream().reduce(0.0, Double::sum) <= 1.0, "Sum of percentages must be <= 1.0");
        this.bins = bins.stream().map(Bin::new).collect(Collectors.toList());
    }
    
    @Override
    public synchronized boolean tryAcquire(Integer index) {
        Preconditions.checkArgument(index >= 0 && index < bins.size(), "Index out of range");
        
        Bin bin = bins.get(index);
        if (busy >= limit && bin.isLimitExceeded()) {
            return false;
        }
        busy++;
        bin.acquire();
        return true;
    }

    @Override
    public synchronized void release(Integer index) {
        Preconditions.checkArgument(index >= 0 && index < bins.size(), "Invalid bin index " + index);
        
        busy--;
        bins.get(index).release();
    }
    
    @Override
    public synchronized void setLimit(int newLimit) { 
        if (this.limit != newLimit) {
            this.limit = newLimit;
            bins.forEach(bin -> bin.updateLimit(newLimit));
        }
    }

    private static class Bin {
        double pct;
        int limit;
        int busy;
        
        public Bin(double pct) {
            this.pct = pct;
        }
        
        public void updateLimit(int totalLimit) {
            // Calculate this bin's limit while rounding up and ensuring the value
            // is at least 1.  With this technique the sum of bin limits may end up being 
            // higher than the concurrency limit.
            this.limit = (int)Math.max(1, Math.ceil(totalLimit * pct));
        }

        public boolean isLimitExceeded() {
            return busy >= limit;
        }

        public void acquire() {
            busy++;
        }
        
        public void release() {
            busy--;
        }

        @Override
        public String toString() {
            return "Bin [pct=" + pct + ", limit=" + limit + ", busy=" + busy + "]";
        }
    }

    public int getBinBusyCount(int index) {
        Preconditions.checkArgument(index >= 0 && index < bins.size(), "Invalid bin index " + index);
        return bins.get(index).busy;
    }
    
    public int getBinLimit(int index) {
        Preconditions.checkArgument(index >= 0 && index < bins.size(), "Invalid bin index " + index);
        return bins.get(index).limit;
    }
    
    public int getBusyCount() {
        return busy;
    }
    
    @Override
    public String toString() {
        final int maxLen = 10;
        return "PercentageStrategy [" + bins.subList(0, Math.min(bins.size(), maxLen)) + "]";
    }
}
