package com.netflix.concurrency.limits;

import java.util.function.Consumer;

/**
 * Contract for an algorithm that calculates a concurrency limit based on 
 * rtt measurements
 */
public interface Limit {
    /**
     * @return Current estimated limit
     */
    int getLimit();

    /**
     * Register a callback to receive notification whenever the limit is updated to a new value
     * @param consumer
     */
    void notifyOnChange(Consumer<Integer> consumer);

    /**
     * Update the limiter with a sample
     * @param startTime
     * @param rtt
     * @param inflight
     * @param didDrop
     */
    void onSample(long startTime, long rtt, int inflight, boolean didDrop);
}
