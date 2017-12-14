package com.netflix.concurrency.limits;

/**
 * Contract for enforcing a concurrency limit with optional partitioning 
 * of the limit.
 * 
 * @param <ContextT> Context type used to partition the limit.  Void if none.
 */
public interface Strategy<ContextT> {
    /**
     * Try to acquire a token from the limiter.
     * 
     * @param context Context of the request for partitioned limits
     * @return True if successful or false if the limit has been reached.  Must call
     *  release() if true.
     */
    boolean tryAcquire(ContextT context);
    
    /**
     * Release a previously acquired limit
     * @param context Context of the request for partitioned limits
     */
    void release(ContextT context);
    
    /**
     * Update the strategy with a new limit
     * @param limit
     */
    void setLimit(int limit);
}
