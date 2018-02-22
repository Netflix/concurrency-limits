package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

/**
 * Non dynamic limit with fixed value
 */
public final class FixedLimit implements Limit {

    private final int limit;

    public static FixedLimit of(int limit) {
        return new FixedLimit(limit);
    }
    
    private FixedLimit(int limit) {
        this.limit = limit;
    }
    
    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public void update(long rtt, int maxInFlight) {
    }

    @Override
    public void drop() {
    }

    @Override
    public String toString() {
        return "FixedLimit [limit=" + limit + "]";
    }
}
