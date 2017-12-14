package com.netflix.concurrency.limits.limit;

import com.netflix.concurrency.limits.Limit;

/**
 * Non dynamic limit with fixed value
 */
public final class FixedLimit implements Limit {

    private final int limit;

    public FixedLimit(int limit) {
        this.limit = limit;
    }
    
    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public int update(long rtt) {
        return limit;
    }

    @Override
    public int drop() {
        return limit;
    }
}
