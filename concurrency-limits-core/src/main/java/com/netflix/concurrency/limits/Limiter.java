package com.netflix.concurrency.limits;

import java.util.Optional;

/**
 * Contract for a concurrency limiter.  The caller is expected to call acquire() for each request
 * and must also release the returned listener when the operation completes.  Releasing the Listener
 * may trigger an update to the concurrency limit based on error rate or latency measurement.
 * 
 * @param <ContextT> Some limiters take a context to perform more fine grained limits.
 */
public interface Limiter<ContextT> {
    /**
     */
    interface Listener {
        /**
         * Notification that the operation succeeded and internally measured latency should be 
         * used as an RTT sample
         */
        void onSuccess();
        
        /**
         * The operation failed before any meaningful RTT measurement could be made and should
         * be ignored to not introduce an artificially low RTT
         */
        void onIgnore();
        
        /**
         * The request failed and was dropped due to being rejected by an external limit or hitting
         * a timeout.  Loss based {@link Limit} implementations will likely do an aggressive
         * reducing in limit when this happens.
         */
        void onDropped();
    }
    
    /**
     * Acquire a token from the limiter.  Returns an Optional.empty() if the limit has been exceeded.
     * If acquired the caller must call one of the Listener methods when the operation has been completed
     * to release the count.
     * 
     * @param context Context for the request
     * @return Optional.empty() if limit exceeded.
     */
    Optional<Listener> acquire(ContextT context);
}
