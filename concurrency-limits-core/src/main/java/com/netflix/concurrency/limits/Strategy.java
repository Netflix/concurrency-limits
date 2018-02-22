package com.netflix.concurrency.limits;

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
         * @return true if acquired or false if limit has been reached
         */
        boolean isAcquired();
        
        /**
         * @return Get number of pending requests
         */
        int getInFlightCount();
        
        /**
         * Release the acquired token and decrement the current inflight count.
         */
        void release();
        
        public static Token newNotAcquired(int inFlight) {
            return new Token() {
                @Override
                public boolean isAcquired() {
                    return false;
                }

                @Override
                public int getInFlightCount() {
                    return inFlight;
                }

                @Override
                public void release() {
                }
            };
        }
        
        public static Token newAcquired(int inFlight, Runnable release) {
            return new Token() {
                @Override
                public boolean isAcquired() {
                    return true;
                }

                @Override
                public int getInFlightCount() {
                    return inFlight;
                }

                @Override
                public void release() {
                    release.run();
                }
            };
        }
    }
    
    /**
     * Try to acquire a token from the limiter.
     * 
     * @param context Context of the request for partitioned limits
     * @return Optional.empty() if limit exceeded or a {@link Token} that must be released when
     *  the operation completes.
     */
    Token tryAcquire(ContextT context);
    
    /**
     * Update the strategy with a new limit
     * @param limit
     */
    void setLimit(int limit);
}
