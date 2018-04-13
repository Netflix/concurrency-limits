package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

/**
 * {@link Limit} to be used mostly for testing where the limit can be manually
 * adjusted.
 */
public class SettableLimit implements Limit {

    private int limit;

    public static SettableLimit startingAt(int limit) {
        return new SettableLimit(limit);
    }
    
    public SettableLimit(int limit) {
        this.limit = limit;
    }
    
    @Override
    public synchronized int getLimit() {
        return limit;
    }

    @Override
    public void update(SampleWindow sample) {
    }
    
    public synchronized void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public String toString() {
        return "SettableLimit [limit=" + limit + "]";
    }
}
