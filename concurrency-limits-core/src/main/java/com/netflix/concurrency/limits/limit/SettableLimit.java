package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

/**
 * {@link Limit} to be used mostly for testing where the limit can be manually
 * adjusted.
 */
public class SettableLimit extends AbstractLimit {

    public static SettableLimit startingAt(int limit) {
        return new SettableLimit(limit);
    }
    
    public SettableLimit(int limit) {
        super(limit);
    }
    
    @Override
    protected int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        return getLimit();
    }
    
    public synchronized void setLimit(int limit) {
        super.setLimit(limit);
    }

    @Override
    public String toString() {
        return "SettableLimit [limit=" + getLimit() + "]";
    }
}
