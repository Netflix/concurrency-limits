package com.netflix.concurrency.limits;

/**
 * Contract for an algorithm that calculates a concurrency limit based on 
 * rtt measurements
 */
public interface Limit {
    /**
     * Details of the current sample window
     */
    interface SampleWindow {
        /**
         * @return Candidate RTT in the sample window. This is traditionally the minimum rtt.
         */
        long getCandidateRttNanos();
        
        /**
         * @return Maximum number of inflight observed during the sample window
         */
        int getMaxInFlight();
        
        /**
         * @return Number of observed RTTs in the sample window
         */
        int getSampleCount();
        
        /**
         * @return True if there was a timeout
         */
        boolean didDrop();
    }
    
    /**
     * @return Current estimated limit
     */
    int getLimit();
    
    /**
     * Update the concurrency limit using a new rtt sample
     * 
     * @param sample Data from the last sampling window such as RTT
     */
    void update(SampleWindow sample);
}
