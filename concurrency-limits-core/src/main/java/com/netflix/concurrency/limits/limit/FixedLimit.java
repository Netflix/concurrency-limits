package com.netflix.concurrency.limits.limit;

/**
 * Non dynamic limit with fixed value
 */
public final class FixedLimit extends AbstractLimit {

    public static FixedLimit of(int limit) {
        return new FixedLimit(limit);
    }
    
    private FixedLimit(int limit) {
        super(limit);
    }

    @Override
    public int _update(long startTime, long rtt, int inflight, boolean didDrop) {
        return getLimit();
    }
    
    @Override
    public String toString() {
        return "FixedLimit [limit=" + getLimit() + "]";
    }
}
