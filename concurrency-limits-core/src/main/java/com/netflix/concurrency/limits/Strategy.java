package com.netflix.concurrency.limits;

import java.util.Optional;

/**
 * Contract for enforcing a concurrency limit with optional partitioning 
 * of the limit.
 * 
 * @param <ContextT> Context type used to partition the limit.  Void if none.
 */
public interface Strategy<ContextT> {
    /**
     * Representation of a single acquired Token from the strategy.  
     */
    interface Token {
        /**
         * Release the acquired token and decrement the current inflight count.
         */
        void release();
    }
    
    /**
     * Try to acquire a token from the limiter.
     * 
     * @param context Context of the request for partitioned limits
     * @return Optional.empty() if limit exceeded or a {@link Token} that must be released when
     *  the operation completes.
     */
    Optional<Token> tryAcquire(ContextT context);
    
    /**
     * Update the strategy with a new limit
     * @param limit
     */
    void setLimit(int limit);
}
